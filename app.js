const express = require("express");
const bodyParser = require("body-parser");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");
const Ajv = require("ajv");
const Redis = require("ioredis");
const jwt = require("jsonwebtoken");
const { OAuth2Client } = require("google-auth-library");
const jwksClient = require("jwks-rsa");

const { Client } = require("@elastic/elasticsearch");
const amqp = require("amqplib");

// Initialize Express App
const app = express();
const port = 3002;
app.use(bodyParser.json());

// Initialize Redis Client (Key/Value Store)
const redis = new Redis({
  host: "localhost",
  port: 6379,
});

// Initialize Elasticsearch Client with compatibility settings
const esClient = new Client({
  node: "http://localhost:9200",
  // Use older API compatibility version
  apiVersion: "7.x",
  // Disable sniffing - important for Docker setups
  sniffOnStart: false,
  sniffOnConnectionFault: false,
  // More robust request settings
  maxRetries: 5,
  requestTimeout: 60000,

  ssl: {
    rejectUnauthorized: false, // Helpful if you're using self-signed certs
  },
});

// Initialize RabbitMQ Connection
let channel;
async function setupQueue() {
  try {
    const connection = await amqp.connect("amqp://localhost:5672");
    channel = await connection.createChannel();
    await channel.assertQueue("indexingQueue", { durable: true });
    console.log("✅ RabbitMQ connected. Queue 'indexingQueue' is ready.");

    // Start consuming after successfully setting up the queue
    await consumeQueue();
  } catch (error) {
    console.error("❌ RabbitMQ Connection Error:", error);
    // Retry connection after a delay
    console.log("⏳ Retrying RabbitMQ connection in 5 seconds...");
    setTimeout(setupQueue, 5000);
  }
}
async function initializeIndex() {
  try {
    // Check if the index exists
    const indexExists = await esClient.indices.exists({ index: "plans" });

    // Delete existing index if it exists
    if (indexExists.body) {
      console.log("⏳ Deleting existing 'plans' index...");
      await esClient.indices.delete({ index: "plans" });
      console.log("✅ Deleted existing 'plans' index.");
    }

    // Create a new index with proper join field mapping
    console.log("⏳ Creating new 'plans' index with join mapping...");
    await esClient.indices.create({
      index: "plans",
      body: {
        mappings: {
          properties: {
            join_field: {
              type: "join",
              relations: {
                plan: ["linkedPlanService", "plancostshare"],
                linkedPlanService: ["childOfLinkedPlanService"],
              },
            },
          },
        },
      },
    });
    console.log("✅ Created 'plans' index with parent-child mapping.");
  } catch (err) {
    if (
      err.meta &&
      err.meta.body &&
      err.meta.body.error &&
      err.meta.body.error.type === "resource_already_exists_exception"
    ) {
      console.warn("⚠️ Tried to create 'plans' index, but it already exists.");
    } else {
      console.error("❌ Error creating 'plans' index:", err);
    }
  }
}
// Initialize Elasticsearch index with robust error handling
// async function initializeIndex() {
//   try {
//     // First check if we can connect to Elasticsearch
//     console.log("Checking Elasticsearch connection...");
//     const pingResult = await esClient.ping();
//     console.log("✅ Elasticsearch connection successful:", pingResult);

//     // Then try to check if index exists
//     try {
//       const indexExists = await esClient.indices.exists({ index: "plans" });
//       console.log("Index check result:", indexExists);

//       if (!indexExists.body) {
//         console.log("⏳ Creating new 'plans' index with join mapping...");
//         try {
//           await esClient.indices.create({
//             index: "plans",
//             body: {
//               mappings: {
//                 properties: {
//                   join_field: {
//                     type: "join",
//                     relations: {
//                       plan: ["linkedPlanService", "plancostshare"],
//                       linkedPlanService: ["childOfLinkedPlanService"],
//                     },
//                   },
//                 },
//               },
//             },
//           });
//           console.log("✅ Created 'plans' index with parent-child mapping.");
//         } catch (createErr) {
//           console.error("Error creating index:", createErr);
//           if (
//             createErr.meta &&
//             createErr.meta.body &&
//             createErr.meta.body.error &&
//             createErr.meta.body.error.type ===
//               "resource_already_exists_exception"
//           ) {
//             console.warn("⚠️ Index already exists, proceeding anyway.");
//           } else {
//             throw createErr;
//           }
//         }
//       } else {
//         console.log("ℹ️ 'plans' index already exists.");
//       }
//     } catch (checkErr) {
//       console.warn("⚠️ Could not check if index exists:", checkErr.message);
//       console.log("Attempting to create index anyway...");

//       try {
//         await esClient.indices.create({
//           index: "plans",
//           body: {
//             mappings: {
//               properties: {
//                 join_field: {
//                   type: "join",
//                   relations: {
//                     plan: ["linkedPlanService", "plancostshare"],
//                     linkedPlanService: ["childOfLinkedPlanService"],
//                   },
//                 },
//               },
//             },
//           },
//           // Ignore error if index already exists
//           ignore: [400],
//         });
//         console.log("✅ Attempted to create 'plans' index.");
//       } catch (forceCreateErr) {
//         console.warn(
//           "⚠️ Error during index creation attempt:",
//           forceCreateErr.message
//         );
//         // Continue anyway, as we'll try to use the index
//       }
//     }
//   } catch (err) {
//     console.error("❌ Error connecting to Elasticsearch:", err.message);
//     console.log("⚠️ Will continue without Elasticsearch functionality");
//   }
// }

// JSON Schema for Validation
const schema = {
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
      required: ["deductible", "_org", "copay", "objectId", "objectType"],
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
            required: ["_org", "objectId", "objectType", "name"],
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
            required: ["deductible", "_org", "copay", "objectId", "objectType"],
          },
          _org: { type: "string" },
          objectId: { type: "string" },
          objectType: { type: "string" },
        },
        required: [
          "linkedService",
          "planserviceCostShares",
          "_org",
          "objectId",
          "objectType",
        ],
      },
    },
    _org: { type: "string" },
    objectId: { type: "string" },
    objectType: { type: "string" },
    planType: { type: "string" },
    creationDate: { type: "string" },
  },
  required: [
    "planCostShares",
    "linkedPlanServices",
    "_org",
    "objectId",
    "objectType",
    "planType",
    "creationDate",
  ],
};

// Initialize AJV
const ajv = new Ajv();
const validate = ajv.compile(schema);

// Generate ETag
const generateEtag = (data) => {
  return crypto.createHash("md5").update(JSON.stringify(data)).digest("hex");
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
    return res.status(401).json({
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

// RabbitMQ Consumer: Process messages for Elasticsearch indexing
// / RabbitMQ Consumer: Index Data into Elasticsearch
// async function consumeQueue() {
//   if (!channel) {
//     console.error("❌ RabbitMQ channel not initialized.");
//     return;
//   }

//   channel.consume("indexingQueue", async (msg) => {
//     if (msg !== null) {
//       try {
//         const data = JSON.parse(msg.content.toString());
//         console.log(`Processing message for plan ${data.objectId}`);

//         try {
//           // Try to delete existing document first for clean indexing
//           try {
//             await esClient.delete({
//               index: "plans",
//               id: data.objectId,
//             });
//             console.log(`Deleted existing document for ${data.objectId}`);
//           } catch (deleteErr) {
//             // Ignore 404 errors (document not found)
//             if (deleteErr.meta && deleteErr.meta.statusCode !== 404) {
//               console.log(`Warning during delete: ${deleteErr.message}`);
//             }
//           }

//           // FIXED: Properly format join_field as an object
//           await esClient.index({
//             index: "plans",
//             id: data.objectId,
//             body: {
//               ...data,
//               join_field: {
//                 name: "plan", // Changed from "plan" string to {name: "plan"} object
//               },
//             },
//             refresh: true, // Force refresh for immediate visibility
//           });

//           // Index each "linkedPlanService" as a child
//           if (
//             data.linkedPlanServices &&
//             Array.isArray(data.linkedPlanServices)
//           ) {
//             for (const service of data.linkedPlanServices) {
//               // Try to delete existing child document
//               try {
//                 await esClient.delete({
//                   index: "plans",
//                   id: service.objectId,
//                 });
//               } catch (deleteErr) {
//                 // Ignore 404 errors
//                 if (deleteErr.meta && deleteErr.meta.statusCode !== 404) {
//                   console.log(
//                     `Warning during child delete: ${deleteErr.message}`
//                   );
//                 }
//               }

//               // FIXED: Properly format child join_field
//               await esClient.index({
//                 index: "plans",
//                 id: service.objectId,
//                 routing: data.objectId, // route to parent
//                 body: {
//                   ...service,
//                   join_field: {
//                     name: "linkedPlanService",
//                     parent: data.objectId,
//                   },
//                 },
//                 refresh: true,
//               });
//             }
//           }

//           // Index planCostShares as a child document
//           if (data.planCostShares) {
//             // Try to delete existing planCostShares child document
//             try {
//               await esClient.delete({
//                 index: "plans",
//                 id: data.planCostShares.objectId,
//               });
//             } catch (deleteErr) {
//               // Ignore 404 errors
//               if (deleteErr.meta && deleteErr.meta.statusCode !== 404) {
//                 console.log(
//                   `Warning during planCostShares delete: ${deleteErr.message}`
//                 );
//               }
//             }

//             // Index planCostShares as a child document
//             await esClient.index({
//               index: "plans",
//               id: data.planCostShares.objectId,
//               routing: data.objectId, // route to parent
//               body: {
//                 ...data.planCostShares,
//                 join_field: {
//                   name: "plancostshare",
//                   parent: data.objectId,
//                 },
//               },
//               refresh: true,
//             });
//           }
//         } catch (esError) {
//           console.warn(`⚠️ Elasticsearch indexing error: ${esError.message}`);
//           if (esError.meta && esError.meta.body) {
//             console.warn("Root causes:");
//             console.warn(JSON.stringify(esError.meta.body.error, null, 8));
//           }
//           console.log(
//             "✅ Data was saved to Redis, but Elasticsearch indexing failed"
//           );
//         }

//         // Acknowledge the message regardless of Elasticsearch result
//         // since the data is safely stored in Redis
//         channel.ack(msg);
//         console.log(`✅ Processed plan ${data.objectId}`);
//       } catch (error) {
//         console.error(`❌ Error processing message:`, error);

//         // Check message headers for retry count
//         const headers = msg.properties.headers || {};
//         const retryCount = (headers["x-retry-count"] || 0) + 1;

//         if (retryCount > 3) {
//           console.warn(
//             `⚠️ Message failed after ${retryCount} attempts, discarding`
//           );
//           // Acknowledge to remove from queue after too many failures
//           channel.ack(msg);
//         } else {
//           // Nack with requeue but limit retries
//           channel.nack(msg, false, true);
//           console.log(`⚠️ Message requeued for retry attempt ${retryCount}`);
//         }
//       }
//     }
//   });
// }
async function consumeQueue() {
  if (!channel) {
    console.error("❌ RabbitMQ channel not initialized.");
    return;
  }

  channel.consume("indexingQueue", async (msg) => {
    if (msg !== null) {
      try {
        const data = JSON.parse(msg.content.toString());

        // Index the parent "plan" with CORRECT join_field format
        await esClient.index({
          index: "plans",
          id: data.objectId,
          body: {
            ...data,
            join_field: {
              name: "plan", // FIXED: Changed from string to object format
            },
          },
        });

        // Index planCostShares as a child document
        if (data.planCostShares && data.planCostShares.objectId) {
          await esClient.index({
            index: "plans",
            id: data.planCostShares.objectId,
            routing: data.objectId, // Important for parent-child relationships
            body: {
              ...data.planCostShares,
              join_field: {
                name: "plancostshare",
                parent: data.objectId,
              },
            },
          });
        }

        // Index each "linkedPlanService" as a child (same as your friend's code)
        if (data.linkedPlanServices && Array.isArray(data.linkedPlanServices)) {
          for (const service of data.linkedPlanServices) {
            await esClient.index({
              index: "plans",
              id: service.objectId,
              routing: data.objectId, // route to parent
              body: {
                ...service,
                join_field: {
                  name: "linkedPlanService",
                  parent: data.objectId,
                },
              },
            });

            // Add the childOfLinkedPlanService as in your friend's code
            await esClient.index({
              index: "plans",
              id: `${service.objectId}-child`,
              routing: service.objectId, // routing must be the parent's ID
              body: {
                dummyField: "child doc under linkedPlanService",
                join_field: {
                  name: "childOfLinkedPlanService",
                  parent: service.objectId,
                },
              },
            });
          }
        }

        channel.ack(msg);
      } catch (error) {
        console.error(`❌ Error processing message:`, error);
        channel.ack(msg); // Still acknowledge to avoid blocking the queue
      }
    }
  });
}
// CREATE: Store Data in Redis & Index in Elasticsearch
app.post("/v1/plan", verifyGoogleToken, async (req, res) => {
  const data = req.body;

  // Validate data against schema
  if (!validate(data)) {
    return res
      .status(400)
      .json({ error: "Validation failed", details: validate.errors });
  }

  // Generate ETag and add it to the data
  const etag = generateEtag(data);
  data.etag = etag;

  try {
    // Store in Redis (Key: objectId, Value: JSON String)
    await redis.set(data.objectId, JSON.stringify(data));

    // Queue indexing operation if RabbitMQ channel is available
    if (channel) {
      channel.sendToQueue("indexingQueue", Buffer.from(JSON.stringify(data)), {
        persistent: true,
        headers: {
          operation: "create",
          timestamp: Date.now(),
        },
      });
      console.log(`Plan ${data.objectId} queued for indexing`);
    } else {
      console.error("❌ RabbitMQ channel not available for sending message");
    }

    res.set({
      "X-Powered-By": "Express",
      Etag: etag,
      "Content-Type": "application/json",
    });

    return res
      .status(201)
      .json({ id: data.objectId, message: "Plan created successfully" });
  } catch (error) {
    console.error("❌ Error creating plan:", error);
    return res.status(500).json({ error: "Failed to create plan" });
  }
});

// PATCH: Update an existing plan
app.patch("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
  const planId = req.params.id;
  const data = req.body;

  try {
    // Retrieve the current plan from Redis
    const planData = await redis.get(planId);
    if (!planData) {
      return res.status(404).json({ error: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    // Get If-Match ETag from the request header
    const clientEtag = req.header("If-Match");

    // If ETag does not match, reject the update
    if (!clientEtag || clientEtag !== plan.etag) {
      return res
        .status(412)
        .json({ error: "ETag mismatch. The resource has been modified." });
    }

    // Merge the updated fields into the existing plan
    Object.keys(data).forEach((key) => {
      if (Array.isArray(data[key]) && Array.isArray(plan[key])) {
        // Merge array elements based on objectId
        data[key].forEach((updatedItem) => {
          const existingItemIndex = plan[key].findIndex(
            (item) => item.objectId === updatedItem.objectId
          );
          if (existingItemIndex !== -1) {
            // Merge updated fields for existing objects
            plan[key][existingItemIndex] = {
              ...plan[key][existingItemIndex],
              ...updatedItem,
            };
          } else {
            // Add new object if not found
            plan[key].push(updatedItem);
          }
        });
      } else {
        // Directly update scalar or object fields
        plan[key] = data[key];
      }
    });

    // Regenerate the ETag for the updated plan
    const newEtag = generateEtag(plan);
    plan.etag = newEtag; // Ensure the etag is included in the stored plan

    // Store the updated plan back in Redis
    await redis.set(planId, JSON.stringify(plan));

    // Queue indexing operation for Elasticsearch
    if (channel) {
      try {
        // Make sure the channel is connected
        if (channel.connection && channel.connection.isOpen) {
          console.log(
            `Queuing updated plan ${planId} for Elasticsearch indexing`
          );

          channel.sendToQueue(
            "indexingQueue",
            Buffer.from(JSON.stringify(plan)),
            {
              persistent: true,
              contentType: "application/json",
              // Add message properties to help with debugging
              headers: {
                operation: "update",
                timestamp: Date.now(),
              },
            }
          );

          console.log(`✅ Updated plan ${planId} queued for indexing`);
        } else {
          console.error("❌ RabbitMQ channel connection is closed");
          // Attempt to reconnect
          await setupQueue();
          if (channel && channel.connection && channel.connection.isOpen) {
            channel.sendToQueue(
              "indexingQueue",
              Buffer.from(JSON.stringify(plan)),
              {
                persistent: true,
                headers: {
                  operation: "update",
                  timestamp: Date.now(),
                },
              }
            );
            console.log(
              `✅ After reconnect: Updated plan ${planId} queued for indexing`
            );
          }
        }
      } catch (queueError) {
        console.error(
          "❌ Error queuing message for Elasticsearch:",
          queueError
        );
        console.log(
          "⚠️ Plan updated in Redis but not queued for Elasticsearch"
        );
      }
    } else {
      console.error("❌ RabbitMQ channel not available for sending message");
    }

    res.set({
      "X-Powered-By": "Express",
      Etag: newEtag,
      "Content-Type": "application/json",
    });

    return res.status(200).json(plan);
  } catch (error) {
    console.error("❌ Error updating plan:", error);
    return res.status(500).json({ error: "Failed to update plan" });
  }
});

// GET: Retrieve Data from Redis
app.get("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
  try {
    const planData = await redis.get(req.params.id);
    if (!planData) {
      return res.status(404).json({ error: "Plan not found" });
    }

    const plan = JSON.parse(planData);
    const clientEtag = req.header("If-None-Match");

    if (clientEtag && clientEtag === plan.etag) {
      return res.status(304).end(); // If the ETag matches, return 304 NOT MODIFIED
    }

    res.set({
      "X-Powered-By": "Express",
      Etag: plan.etag,
      "Content-Type": "application/json",
    });

    return res.status(200).json(plan); // Return the plan with the ETag
  } catch (error) {
    console.error("❌ Error retrieving plan:", error);
    return res.status(500).json({ error: "Failed to retrieve plan" });
  }
});

// DELETE: Remove Data & Perform Cascading Deletes
app.delete("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
  const planId = req.params.id;

  try {
    const planData = await redis.get(planId);
    if (!planData) {
      return res.status(404).json({ error: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    // Cascading delete: Remove child records
    if (plan.linkedPlanServices) {
      for (const service of plan.linkedPlanServices) {
        await redis.del(service.objectId);

        // Try to delete from Elasticsearch too
        try {
          await esClient.delete({
            index: "plans",
            id: service.objectId,
            ignore: [404], // Ignore if not found
          });
        } catch (esError) {
          console.warn(
            `⚠️ Could not delete child document from Elasticsearch: ${esError.message}`
          );
          // Continue with deletion process
        }
      }
    }

    // Delete plan cost shares if it exists
    if (plan.planCostShares && plan.planCostShares.objectId) {
      await redis.del(plan.planCostShares.objectId);

      try {
        await esClient.delete({
          index: "plans",
          id: plan.planCostShares.objectId,
          ignore: [404],
        });
      } catch (esError) {
        console.warn(
          `⚠️ Could not delete planCostShares from Elasticsearch: ${esError.message}`
        );
      }
    }

    // Delete the main plan record
    await redis.del(planId);

    try {
      await esClient.delete({
        index: "plans",
        id: planId,
        ignore: [404],
      });
    } catch (esError) {
      console.warn(
        `⚠️ Could not delete document from Elasticsearch: ${esError.message}`
      );
      // Continue since Redis deletion was successful
    }

    res.status(204).end();
  } catch (error) {
    console.error("❌ Error deleting plan:", error);
    return res.status(500).json({ error: "Failed to delete plan" });
  }
});

// SEARCH: Query Data in Elasticsearch with fallback to Redis
app.get("/v1/search", verifyGoogleToken, async (req, res) => {
  const query = req.query.q;

  if (!query) {
    return res
      .status(400)
      .json({ error: "Search query parameter 'q' is required" });
  }

  try {
    try {
      // First try Elasticsearch
      const result = await esClient.search({
        index: "plans",
        body: {
          query: {
            match: { planType: query },
          },
        },
      });

      return res.json(result.body.hits.hits.map((hit) => hit._source));
    } catch (esError) {
      console.warn(`⚠️ Elasticsearch search failed: ${esError.message}`);
      console.log("Falling back to Redis search...");

      // Fallback to Redis search (slower but more reliable)
      const keys = await redis.keys("*");
      const results = [];

      // Get all plan data from Redis
      for (const key of keys) {
        const planData = await redis.get(key);
        if (planData) {
          try {
            const plan = JSON.parse(planData);
            // Simple matching on planType
            if (
              plan.planType &&
              plan.planType.toLowerCase().includes(query.toLowerCase())
            ) {
              results.push(plan);
            }
          } catch (e) {
            console.warn(
              `Error parsing Redis data for key ${key}: ${e.message}`
            );
          }
        }
      }

      return res.json(results);
    }
  } catch (error) {
    console.error("❌ Error searching plans:", error);
    return res.status(500).json({ error: "Failed to search plans" });
  }
});

// Add a new endpoint to directly search Elasticsearch with all operators
app.get("/v1/elasticsearch/search", verifyGoogleToken, async (req, res) => {
  try {
    // Use the match_all query to return everything in the index
    const result = await esClient.search({
      index: "plans",
      body: {
        query: {
          match_all: {},
        },
        size: req.query.size || 10,
      },
    });

    return res.json({
      total: result.body.hits.total.value,
      hits: result.body.hits.hits.map((hit) => hit._source),
    });
  } catch (error) {
    console.error("❌ Error performing direct Elasticsearch search:", error);
    return res.status(500).json({
      error: "Failed to search Elasticsearch",
      details: error.message,
    });
  }
});
// Add this endpoint to your Express application

// Endpoint for searching plans by child document copay
app.get("/v1/plans/by-copay", verifyGoogleToken, async (req, res) => {
  const minCopay = parseFloat(req.query.min || "1");

  try {
    // First, let's verify that plancostshare documents exist
    const verificationResult = await esClient.search({
      index: "plans",
      body: {
        size: 0,
        query: {
          match: {
            "join_field.name": "plancostshare",
          },
        },
      },
    });

    const plancostshareCount = verificationResult.body.hits.total.value;
    console.log(`Found ${plancostshareCount} plancostshare documents`);

    if (plancostshareCount === 0) {
      return res.json({
        message: "No plancostshare documents found in the index",
        matchingPlans: [],
      });
    }

    // Now execute the parent-child query
    const result = await esClient.search({
      index: "plans",
      body: {
        query: {
          has_child: {
            type: "plancostshare",
            query: {
              range: {
                copay: {
                  gte: minCopay,
                },
              },
            },
          },
        },
      },
    });

    // Extract and return the matching plans
    const matchingPlans = result.body.hits.hits.map((hit) => hit._source);

    return res.json({
      total: result.body.hits.total.value,
      message: `Found ${result.body.hits.total.value} plans with copay >= ${minCopay}`,
      matchingPlans,
    });
  } catch (error) {
    console.error("❌ Error executing Elasticsearch query:", error);

    // Extract detailed error information for troubleshooting
    let errorDetails = error.message;
    if (error.meta && error.meta.body) {
      errorDetails = JSON.stringify(error.meta.body, null, 2);
    }

    return res.status(500).json({
      error: "Failed to execute search",
      details: errorDetails,
    });
  }
});

// Diagnostic endpoint to check document types in the index
app.get(
  "/v1/elasticsearch/document-types",
  verifyGoogleToken,
  async (req, res) => {
    try {
      const result = await esClient.search({
        index: "plans",
        body: {
          size: 0,
          aggs: {
            doc_types: {
              terms: {
                field: "join_field.name",
                size: 10,
              },
            },
          },
        },
      });

      return res.json({
        documentTypes: result.body.aggregations.doc_types.buckets,
      });
    } catch (error) {
      console.error("❌ Error getting document types:", error);
      return res.status(500).json({
        error: "Failed to get document types",
        details: error.message,
      });
    }
  }
);

// Helper function to verify a specific plan's structure in Elasticsearch
app.get(
  "/v1/plan/:id/verify-elasticsearch",
  verifyGoogleToken,
  async (req, res) => {
    const planId = req.params.id;

    try {
      // Get the plan from Redis
      const planData = await redis.get(planId);
      if (!planData) {
        return res.status(404).json({ error: "Plan not found in Redis" });
      }

      const plan = JSON.parse(planData);

      // Look for the plan in Elasticsearch
      const planResult = await esClient.search({
        index: "plans",
        body: {
          query: {
            match: {
              objectId: planId,
            },
          },
        },
      });

      const planInEs =
        planResult.body.hits.total.value > 0
          ? planResult.body.hits.hits[0]._source
          : null;

      // Check for plancostshare children
      let costShareInEs = null;
      if (plan.planCostShares && plan.planCostShares.objectId) {
        try {
          const costShareResult = await esClient.get({
            index: "plans",
            id: plan.planCostShares.objectId,
            routing: planId, // Important! Need parent routing
          });

          costShareInEs = costShareResult.body._source;
        } catch (e) {
          console.log(`Cost share document not found: ${e.message}`);
        }
      }

      // Return verification results
      return res.json({
        plan: {
          inRedis: plan,
          inElasticsearch: planInEs,
          exists: !!planInEs,
        },
        planCostShares: {
          inRedis: plan.planCostShares,
          inElasticsearch: costShareInEs,
          exists: !!costShareInEs,
        },
        verification: {
          indexingNeeded: !planInEs || !costShareInEs,
        },
      });
    } catch (error) {
      console.error("❌ Error verifying Elasticsearch data:", error);
      return res.status(500).json({
        error: "Failed to verify Elasticsearch data",
        details: error.message,
      });
    }
  }
);

// Endpoint to force reindexing of a plan
app.post("/v1/plan/:id/reindex", verifyGoogleToken, async (req, res) => {
  const planId = req.params.id;

  try {
    // Get the plan from Redis
    const planData = await redis.get(planId);
    if (!planData) {
      return res.status(404).json({ error: "Plan not found in Redis" });
    }

    const plan = JSON.parse(planData);

    // Queue the plan for indexing
    if (channel) {
      channel.sendToQueue("indexingQueue", Buffer.from(JSON.stringify(plan)), {
        persistent: true,
        contentType: "application/json",
        headers: {
          operation: "reindex",
          timestamp: Date.now(),
        },
      });

      return res.json({
        message: `Plan ${planId} queued for reindexing`,
      });
    } else {
      return res.status(500).json({
        error: "RabbitMQ channel not available",
      });
    }
  } catch (error) {
    console.error("❌ Error reindexing plan:", error);
    return res.status(500).json({
      error: "Failed to reindex plan",
      details: error.message,
    });
  }
});

// Start the server and initialize connections
async function startServer() {
  try {
    // Initialize connections
    await initializeIndex();
    await setupQueue();

    // Start the Express server
    app.listen(port, () => {
      console.log(`🚀 Server running at http://localhost:${port}`);
      console.log(`📊 Connected to Redis at localhost:6379`);
      console.log(`🔍 Connected to Elasticsearch at http://localhost:9200`);
      console.log(`🐇 Connected to RabbitMQ at localhost:5672`);
    });
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
// const express = require("express");
// const mongoose = require("mongoose");
// const bodyParser = require("body-parser");
// const crypto = require("crypto");
// const jwt = require("jsonwebtoken");
// const jwksClient = require("jwks-rsa");
// const Ajv = require("ajv");

// const app = express();
// const ajv = new Ajv();

// // Middleware
// app.use(bodyParser.json());

// // Connect to MongoDB
// mongoose
//   .connect("mongodb://localhost:27017/healthcare", {
//     useNewUrlParser: true,
//     useUnifiedTopology: true,
//   })
//   .then(() => console.log("✅ MongoDB Connected"))
//   .catch((err) => console.error("❌ MongoDB Connection Error:", err));

// // Define MongoDB Schema (Key-Value Storage)
// const PlanSchema = new mongoose.Schema({}, { strict: false }); // Allows any JSON structure
// const Plan = mongoose.model("Plan", PlanSchema);

// // JSON Schema for Validation
// const jsonSchema = {
//   type: "object",
//   properties: {
//     planCostShares: {
//       type: "object",
//       properties: {
//         deductible: { type: "number" },
//         _org: { type: "string" },
//         copay: { type: "number" },
//         objectId: { type: "string" },
//         objectType: { type: "string" },
//       },
//       required: ["deductible", "copay", "objectId", "objectType"],
//     },
//     linkedPlanServices: {
//       type: "array",
//       items: {
//         type: "object",
//         properties: {
//           linkedService: {
//             type: "object",
//             properties: {
//               _org: { type: "string" },
//               objectId: { type: "string" },
//               objectType: { type: "string" },
//               name: { type: "string" },
//             },
//             required: ["objectId", "objectType", "name"],
//           },
//           planserviceCostShares: {
//             type: "object",
//             properties: {
//               deductible: { type: "number" },
//               _org: { type: "string" },
//               copay: { type: "number" },
//               objectId: { type: "string" },
//               objectType: { type: "string" },
//             },
//             required: ["deductible", "copay", "objectId", "objectType"],
//           },
//           objectId: { type: "string" },
//           objectType: { type: "string" },
//         },
//         required: [
//           "linkedService",
//           "planserviceCostShares",
//           "objectId",
//           "objectType",
//         ],
//       },
//     },
//     objectId: { type: "string" },
//     objectType: { type: "string" },
//     planType: { type: "string" },
//     creationDate: { type: "string" },
//   },
//   required: ["objectId", "objectType", "planType", "creationDate"],
// };

// // ✅ GOOGLE JWT AUTHENTICATION MIDDLEWARE
// const GOOGLE_JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs";
// const client = jwksClient({ jwksUri: GOOGLE_JWKS_URI });

// function getKey(header, callback) {
//   client.getSigningKey(header.kid, (err, key) => {
//     if (err) {
//       console.error("Error getting signing key:", err);
//       return callback(err);
//     }
//     const signingKey = key.publicKey || key.rsaPublicKey;
//     callback(null, signingKey);
//   });
// }

// function verifyGoogleToken(req, res, next) {
//   const authHeader = req.headers.authorization;
//   console.log("Auth header received:", authHeader);

//   if (!authHeader) {
//     return res
//       .status(401)
//       .json({ error: "Unauthorized: No authorization header provided" });
//   }

//   if (!authHeader.startsWith("Bearer ")) {
//     return res
//       .status(401)
//       .json({
//         error: "Unauthorized: Authorization header must start with 'Bearer '",
//       });
//   }

//   const token = authHeader.split(" ")[1];
//   console.log("Token extracted:", token.substring(0, 20) + "..."); // Log first part of token for debugging

//   if (!token) {
//     return res
//       .status(401)
//       .json({ error: "Unauthorized: No token provided after 'Bearer'" });
//   }

//   jwt.verify(token, getKey, { algorithms: ["RS256"] }, (err, decoded) => {
//     if (err) {
//       console.error("Token verification error:", err.name, err.message);
//       return res
//         .status(403)
//         .json({ error: "Forbidden: Invalid token", details: err.message });
//     }

//     req.user = decoded;
//     console.log("Authenticated user:", decoded.email || decoded.sub);
//     next();
//   });
// }

// // Helper function to generate ETag from plan data (SHA-256 hash)
// function generateETag(plan) {
//   const planString = JSON.stringify(plan); // Convert plan object to string
//   return crypto.createHash("sha256").update(planString).digest("hex");
// }

// // ✅ **POST - Create New Plan** (Requires Authentication)
// app.post("/api/v1/plan", verifyGoogleToken, async (req, res) => {
//   const validate = ajv.compile(jsonSchema);
//   if (!validate(req.body)) {
//     return res
//       .status(400)
//       .json({ error: "Invalid JSON structure", details: validate.errors });
//   }

//   try {
//     // 🔍 Check if objectId already exists in MongoDB
//     const existingPlan = await Plan.findOne({ objectId: req.body.objectId });

//     if (existingPlan) {
//       return res
//         .status(409)
//         .json({ error: "Plan with this objectId already exists" });
//     }

//     // Add user information to plan
//     req.body.createdBy = req.user.email || req.user.sub;

//     // ✅ If not found, create a new plan
//     const newPlan = new Plan(req.body);
//     await newPlan.save();

//     // Generate ETag for the new plan
//     const savedPlan = await Plan.findOne({
//       objectId: req.body.objectId,
//     }).lean();
//     const etag = generateETag(savedPlan);

//     res.setHeader("ETag", etag);
//     res.status(201).json({
//       message: "Plan created successfully!",
//       id: newPlan.objectId,
//       etag: etag,
//     });
//   } catch (err) {
//     console.error("Error saving plan:", err);
//     res.status(500).json({ error: "Error saving plan" });
//   }
// });

// // ✅ **GET - Retrieve a Plan (with Conditional Read)**
// app.get("/api/v1/plan/:objectId", async (req, res) => {
//   try {
//     const plan = await Plan.findOne({ objectId: req.params.objectId }).lean();
//     if (!plan) return res.status(404).json({ error: "Plan not found" });

//     // Generate ETag by hashing the plan data
//     const etag = generateETag(plan);

//     // Log the generated ETag
//     console.log("Generated ETag:", etag);

//     // Conditional Read (`If-None-Match` Header)
//     const ifNoneMatch = req.header("If-None-Match");
//     console.log("Received If-None-Match header:", ifNoneMatch);

//     if (ifNoneMatch && ifNoneMatch === etag) {
//       console.log("ETag matches, returning 304 Not Modified");
//       return res.status(304).send();
//     }

//     // Send the plan if it is modified or If-None-Match didn't match
//     res.setHeader("ETag", etag); // Send the ETag header
//     console.log("Returning plan with ETag:", etag);
//     res.status(200).json(plan);
//   } catch (err) {
//     console.error("Error retrieving plan:", err);
//     res.status(500).json({ error: "Error retrieving plan" });
//   }
// });

// // ✅ **PATCH - Update a Plan (with Conditional Update)**
// app.patch("/api/v1/plan/:objectId", verifyGoogleToken, async (req, res) => {
//   try {
//     const plan = await Plan.findOne({ objectId: req.params.objectId }).lean();

//     if (!plan) {
//       return res.status(404).json({ error: "Plan not found" });
//     }

//     // Generate current ETag
//     const currentEtag = generateETag(plan);

//     // Conditional Update (`If-Match` Header)
//     const ifMatch = req.header("If-Match");

//     // If If-Match header is provided, verify it matches current ETag
//     if (ifMatch && ifMatch !== currentEtag) {
//       return res.status(412).json({
//         error: "Precondition Failed: Resource has been modified",
//         currentEtag: currentEtag,
//       });
//     }

//     // Merge the existing plan with the updates
//     const updatedData = { ...plan, ...req.body };

//     // Validate the merged data
//     const validate = ajv.compile(jsonSchema);
//     if (!validate(updatedData)) {
//       return res.status(400).json({
//         error: "Invalid JSON structure after update",
//         details: validate.errors,
//       });
//     }

//     // Add audit info
//     updatedData.lastModifiedBy = req.user.email || req.user.sub;
//     updatedData.lastModifiedAt = new Date().toISOString();

//     // Update the plan
//     await Plan.findOneAndUpdate(
//       { objectId: req.params.objectId },
//       updatedData,
//       { new: true }
//     );

//     // Get the updated plan and generate new ETag
//     const updatedPlan = await Plan.findOne({
//       objectId: req.params.objectId,
//     }).lean();
//     const newEtag = generateETag(updatedPlan);

//     // Return the updated plan with new ETag
//     res.setHeader("ETag", newEtag);
//     res.status(200).json(updatedPlan);
//   } catch (err) {
//     console.error("Error updating plan:", err);
//     res.status(500).json({ error: "Error updating plan" });
//   }
// });

// // ✅ **DELETE - Remove a Plan** (Requires Authentication)
// app.delete("/api/v1/plan/:objectId", verifyGoogleToken, async (req, res) => {
//   try {
//     const result = await Plan.deleteOne({ objectId: req.params.objectId });
//     console.log("Deleting objectId:", req.params.objectId);
//     if (result.deletedCount === 0) {
//       return res.status(404).json({ error: "Plan not found" });
//     }
//     res.status(204).send();
//   } catch (err) {
//     console.error("Error deleting plan:", err);
//     res.status(500).json({ error: "Error deleting plan" });
//   }
// });

// // Start Server
// const PORT = 3000;
// app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
