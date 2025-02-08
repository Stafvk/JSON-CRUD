const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
const crypto = require("crypto");
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

// ✅ **POST - Create New Plan**
app.post("/api/v1/plan", async (req, res) => {
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

    // ✅ If not found, create a new plan
    const newPlan = new Plan(req.body);
    await newPlan.save();
    res
      .status(201)
      .json({ message: "Plan created successfully!", id: newPlan.objectId });
  } catch (err) {
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
    console.log("Received If-None-Match header:", ifNoneMatch); // Log the incoming If-None-Match header

    if (ifNoneMatch && ifNoneMatch === etag) {
      console.log("ETag matches, returning 304 Not Modified");
      return res.status(304).json({ message: "Not Modified" });
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

// ✅ **DELETE - Remove a Plan**
app.delete("/api/v1/plan/:objectId", async (req, res) => {
  try {
    const result = await Plan.deleteOne({ objectId: req.params.objectId });
    console.log("Deleting objectId:", req.params.objectId);
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Plan not found" });
    }
    res.status(204).send();
  } catch (err) {
    res.status(500).json({ error: "Error deleting plan" });
  }
});

// Helper function to generate ETag from plan data (SHA-256 hash)
function generateETag(plan) {
  const planString = JSON.stringify(plan); // Convert plan object to string
  return crypto.createHash("sha256").update(planString).digest("hex");
}

// Start Server
const PORT = 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
