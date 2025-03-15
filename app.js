const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
const crypto = require("crypto");
const jwt = require("jsonwebtoken");
const jwksClient = require("jwks-rsa");
const Ajv = require("ajv");

const app = express();
const ajv = new Ajv();

// Middleware
app.use(bodyParser.json());

// Connect to MongoDB
mongoose
  .connect("mongodb://localhost:27017/healthcare", {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => console.error("❌ MongoDB Connection Error:", err));

// Define MongoDB Schema (Key-Value Storage)
const PlanSchema = new mongoose.Schema({}, { strict: false }); // Allows any JSON structure
const Plan = mongoose.model("Plan", PlanSchema);

// JSON Schema for Validation
const jsonSchema = {
  type: "object",
  properties: {
    planCostShares: {
      type: "object",
      properties: {
        deductible: { type: "number" },
        _org: { type: "string" },
        copay: { type: "number" },
        objectId: { type: "string" },
        objectType: { type: "string" },
      },
      required: ["deductible", "copay", "objectId", "objectType"],
    },
    linkedPlanServices: {
      type: "array",
      items: {
        type: "object",
        properties: {
          linkedService: {
            type: "object",
            properties: {
              _org: { type: "string" },
              objectId: { type: "string" },
              objectType: { type: "string" },
              name: { type: "string" },
            },
            required: ["objectId", "objectType", "name"],
          },
          planserviceCostShares: {
            type: "object",
            properties: {
              deductible: { type: "number" },
              _org: { type: "string" },
              copay: { type: "number" },
              objectId: { type: "string" },
              objectType: { type: "string" },
            },
            required: ["deductible", "copay", "objectId", "objectType"],
          },
          objectId: { type: "string" },
          objectType: { type: "string" },
        },
        required: [
          "linkedService",
          "planserviceCostShares",
          "objectId",
          "objectType",
        ],
      },
    },
    objectId: { type: "string" },
    objectType: { type: "string" },
    planType: { type: "string" },
    creationDate: { type: "string" },
  },
  required: ["objectId", "objectType", "planType", "creationDate"],
};

// ✅ GOOGLE JWT AUTHENTICATION MIDDLEWARE
const GOOGLE_JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs";
const client = jwksClient({ jwksUri: GOOGLE_JWKS_URI });

function getKey(header, callback) {
  client.getSigningKey(header.kid, (err, key) => {
    if (err) {
      console.error("Error getting signing key:", err);
      return callback(err);
    }
    const signingKey = key.publicKey || key.rsaPublicKey;
    callback(null, signingKey);
  });
}

function verifyGoogleToken(req, res, next) {
  const authHeader = req.headers.authorization;
  console.log("Auth header received:", authHeader);

  if (!authHeader) {
    return res
      .status(401)
      .json({ error: "Unauthorized: No authorization header provided" });
  }

  if (!authHeader.startsWith("Bearer ")) {
    return res
      .status(401)
      .json({
        error: "Unauthorized: Authorization header must start with 'Bearer '",
      });
  }

  const token = authHeader.split(" ")[1];
  console.log("Token extracted:", token.substring(0, 20) + "..."); // Log first part of token for debugging

  if (!token) {
    return res
      .status(401)
      .json({ error: "Unauthorized: No token provided after 'Bearer'" });
  }

  jwt.verify(token, getKey, { algorithms: ["RS256"] }, (err, decoded) => {
    if (err) {
      console.error("Token verification error:", err.name, err.message);
      return res
        .status(403)
        .json({ error: "Forbidden: Invalid token", details: err.message });
    }

    req.user = decoded;
    console.log("Authenticated user:", decoded.email || decoded.sub);
    next();
  });
}

// Helper function to generate ETag from plan data (SHA-256 hash)
function generateETag(plan) {
  const planString = JSON.stringify(plan); // Convert plan object to string
  return crypto.createHash("sha256").update(planString).digest("hex");
}

// ✅ **POST - Create New Plan** (Requires Authentication)
app.post("/api/v1/plan", verifyGoogleToken, async (req, res) => {
  const validate = ajv.compile(jsonSchema);
  if (!validate(req.body)) {
    return res
      .status(400)
      .json({ error: "Invalid JSON structure", details: validate.errors });
  }

  try {
    // 🔍 Check if objectId already exists in MongoDB
    const existingPlan = await Plan.findOne({ objectId: req.body.objectId });

    if (existingPlan) {
      return res
        .status(409)
        .json({ error: "Plan with this objectId already exists" });
    }

    // Add user information to plan
    req.body.createdBy = req.user.email || req.user.sub;

    // ✅ If not found, create a new plan
    const newPlan = new Plan(req.body);
    await newPlan.save();

    // Generate ETag for the new plan
    const savedPlan = await Plan.findOne({
      objectId: req.body.objectId,
    }).lean();
    const etag = generateETag(savedPlan);

    res.setHeader("ETag", etag);
    res.status(201).json({
      message: "Plan created successfully!",
      id: newPlan.objectId,
      etag: etag,
    });
  } catch (err) {
    console.error("Error saving plan:", err);
    res.status(500).json({ error: "Error saving plan" });
  }
});

// ✅ **GET - Retrieve a Plan (with Conditional Read)**
app.get("/api/v1/plan/:objectId", async (req, res) => {
  try {
    const plan = await Plan.findOne({ objectId: req.params.objectId }).lean();
    if (!plan) return res.status(404).json({ error: "Plan not found" });

    // Generate ETag by hashing the plan data
    const etag = generateETag(plan);

    // Log the generated ETag
    console.log("Generated ETag:", etag);

    // Conditional Read (`If-None-Match` Header)
    const ifNoneMatch = req.header("If-None-Match");
    console.log("Received If-None-Match header:", ifNoneMatch);

    if (ifNoneMatch && ifNoneMatch === etag) {
      console.log("ETag matches, returning 304 Not Modified");
      return res.status(304).send();
    }

    // Send the plan if it is modified or If-None-Match didn't match
    res.setHeader("ETag", etag); // Send the ETag header
    console.log("Returning plan with ETag:", etag);
    res.status(200).json(plan);
  } catch (err) {
    console.error("Error retrieving plan:", err);
    res.status(500).json({ error: "Error retrieving plan" });
  }
});

// ✅ **PATCH - Update a Plan (with Conditional Update)**
app.patch("/api/v1/plan/:objectId", verifyGoogleToken, async (req, res) => {
  try {
    const plan = await Plan.findOne({ objectId: req.params.objectId }).lean();

    if (!plan) {
      return res.status(404).json({ error: "Plan not found" });
    }

    // Generate current ETag
    const currentEtag = generateETag(plan);

    // Conditional Update (`If-Match` Header)
    const ifMatch = req.header("If-Match");

    // If If-Match header is provided, verify it matches current ETag
    if (ifMatch && ifMatch !== currentEtag) {
      return res.status(412).json({
        error: "Precondition Failed: Resource has been modified",
        currentEtag: currentEtag,
      });
    }

    // Merge the existing plan with the updates
    const updatedData = { ...plan, ...req.body };

    // Validate the merged data
    const validate = ajv.compile(jsonSchema);
    if (!validate(updatedData)) {
      return res.status(400).json({
        error: "Invalid JSON structure after update",
        details: validate.errors,
      });
    }

    // Add audit info
    updatedData.lastModifiedBy = req.user.email || req.user.sub;
    updatedData.lastModifiedAt = new Date().toISOString();

    // Update the plan
    await Plan.findOneAndUpdate(
      { objectId: req.params.objectId },
      updatedData,
      { new: true }
    );

    // Get the updated plan and generate new ETag
    const updatedPlan = await Plan.findOne({
      objectId: req.params.objectId,
    }).lean();
    const newEtag = generateETag(updatedPlan);

    // Return the updated plan with new ETag
    res.setHeader("ETag", newEtag);
    res.status(200).json(updatedPlan);
  } catch (err) {
    console.error("Error updating plan:", err);
    res.status(500).json({ error: "Error updating plan" });
  }
});

// ✅ **DELETE - Remove a Plan** (Requires Authentication)
app.delete("/api/v1/plan/:objectId", verifyGoogleToken, async (req, res) => {
  try {
    const result = await Plan.deleteOne({ objectId: req.params.objectId });
    console.log("Deleting objectId:", req.params.objectId);
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Plan not found" });
    }
    res.status(204).send();
  } catch (err) {
    console.error("Error deleting plan:", err);
    res.status(500).json({ error: "Error deleting plan" });
  }
});

// Start Server
const PORT = 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
