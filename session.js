var aiota = require("aiota-utils");
var amqp = require("amqp");
var crypto = require("crypto");
var jsonValidate = require("jsonschema").validate;
var MongoClient = require("mongodb").MongoClient;
var config = require("./config");

function randomKey(len, chars)
{
    chars = chars || "abcdefghijklmnopqrstuwxyzABCDEFGHIJKLMNOPQRSTUWXYZ0123456789";
    var rnd = crypto.randomBytes(len);
	var value = new Array(len);
	var l = chars.length;

    for (var i = 0; i < len; i++) {
        value[i] = chars[rnd[i] % l]
    };

    return value.join("");
}

function validate(instance, schema)
{
	var v = jsonValidate(instance, schema);

	return (v.errors.length == 0 ? { isValid: true } : { isValid: false, error: v.errors });
}

function createDeviceResponse(db, header, payload, callback)
{
	db.collection("actions", function(err, collection) {
		if (err) {
			callback({ error: err });
			return;
		}

		var now = Date.now();
		var timeout = now + 86400000;
		var maxResends = 3;
		var resendTimeout = 10000;
		var resendAfter = now + resendTimeout;
		
		var obj = { deviceId: header.deviceId, encryption: header.encryption, requestId: header.requestId, action: "response", params: payload, status: 0, createdAt: now, timeoutAt: timeout, progress: [ { timestamp: now, status: "created" } ], resends: { numResends: 0, maxResends: maxResends, resendAfter: resendAfter, resendTimeout: resendTimeout } };
		
		collection.insert(obj, function(err, result) {
			if (err) {
				reply = { error: err, errorCode: 200003 };
			}
			else {
				reply = payload;
			}
			
			callback(reply);
		});
	});
}

function handleNewSession(db, msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			timeout: { type: "integer", minimum: 0, required: true }
		}
	};
		
	var v = validate(msg.body, schema);

	if (v.isValid) {
		db.collection("devices", function(err, collection) {
			if (err) {
				callback({ error: err, errorCode: 200001 });
				return;
			}
			
			collection.findOne({ _id: msg.header.deviceId }, { apps: 1 }, function(err, device) {
				if (err) {
					callback({ error: err, errorCode: 200002 });
					return;
				}

				if (device && device.hasOwnProperty("apps")) {
					if (device.apps.hasOwnProperty(msg.header.encryption.tokencardId)) {
						if (device.apps[msg.header.encryption.tokencardId].hasOwnProperty("status")) {
							if (device.apps[msg.header.encryption.tokencardId].status == "registered") {
								var set = {};
				
								var token = randomKey(24);
								var timeoutAt = (msg.body.timeout > 0 ? Date.now() + (msg.body.timeout * 1000) : 0);
												
								set["apps." + msg.header.encryption.tokencardId + ".session"] = { id: token, timeoutAt: timeoutAt };
					
								collection.update({ _id: msg.header.deviceId }, { $set: set }, function(err, result) {
									if (err) {
										callback({ error: err, errorCode: 200004 });
										return;
									}
									
									callback({ responseType: "session", sessionId: token });
								});
							}
							else {
								callback({ error: "The application status is not equal to 'registered'.", errorCode: 100032 });
							}
						}
						else {
							callback({ error: "The application is wrongly defined on the device.", errorCode: 100003 });
						}
					}
					else {
						callback({ error: "The application has not been registered on this device.", errorCode: 100012 });
					}
				}
				else {
					callback({ error: "The device does not exist." });
				}
			});			
		});
	}
	else {
		callback({ error: v.error });
	}
}

function handleSessionRequest(db, msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			header: {
				type: "object",
				properties: {
					requestId: { type: "string", required: true },
					deviceId: { type: "string", required: true },
					type: { type: "string", enum: [ "session" ], required: true },
					timestamp: { type: "integer", minimum: 0, required: true },
					ttl: { type: "integer", minimum: 0, required: true },
					encryption: {
						type: "object",
						properties: {
							method: { type: "string", required: true },
							tokencardId: { type: "string", required: true }
						},
						required: true
					}
				},
				required: true
			},
			body: {
				type: "object",
				required: true
			}
		}
	};

	var v = validate(msg, schema);

	if (v.isValid) {
		// Get the application definition
		db.collection("applications", function(err, collection) {
			if (err) {
				callback({ error: err, errorCode: 200001 });
				return;
			}
			
			collection.findOne({ _id: msg.header.encryption.tokencardId }, { _id: 0, name: 1, version: 1 }, function(err, app) {
				if (err) {
					callback({ error: err, errorCode: 200002 });
					return;
				}
				else {
					if (app) {
						schema = { 
							type: "object",
							properties: {
								name: { type: "string", required: true },
								version: { 
									type: "object",
									properties: {
										major: { type: "integer", required: true },
										minor: { type: "integer", required: true }
									},
									required: true
								}
							}
						};
	
						v = validate(app, schema);
					
						if (v.isValid) {
							// Check that the message has not expired
							var now = Date.now();
							
							if ((msg.header.ttl > 0) && (now > (msg.header.timestamp + msg.header.ttl * 1000))) {
								createDeviceResponse(db, msg.header, { error: "This message has expired.", errorCode: 100017 }, function(reply) {
									callback(reply);
								});
							}
							else {
								switch(msg.header.type) {
								case "session":		handleNewSession(db, msg, function(reply) {
														createDeviceResponse(db, msg.header, reply, function(reply) {
															callback(reply);
														});
													});
													break;
								default:			createDeviceResponse(db, msg.header, { error: "The message type parameter ('system/" + msg.header.type + "') is not valid.", errorCode: 100018 }, function(reply) {
														callback(reply);
													});
								}
							}
						}
						else {
							callback({ error: v.error, errorCode: 100003 });
						}
					}
					else {
						callback({ error: "The application does not exist.", errorCode: 100016 });
					}
				}
			});
		});
	}
	else {
		callback({ error: v.error, errorCode: 100003 });
	}
}

MongoClient.connect("mongodb://" + config.database.host + ":" + config.database.port + "/aiota", function(err, aiotaDB) {
	if (err) {
		aiota.log(config.processName, config.serverName, aiotaDB, err);
	}
	else {
		aiota.processHeartbeat(config.processName, config.serverName, aiotaDB);
		
		MongoClient.connect("mongodb://" + config.database.host + ":" + config.database.port + "/" + config.database.name, function(err, db) {
			if (err) {
				aiota.log(config.processName, config.serverName, aiotaDB, err);
			}
			else {
				var bus = amqp.createConnection(config.amqp);
				
				bus.on("ready", function() {
					var cl = { group: "system", type: "session" };
					bus.queue(aiota.getQueue(cl), { autoDelete: false, durable: true }, function(queue) {
						queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
							handleSessionRequest(db, msg, function(result) {
								queue.shift();
							});
						});
					});
				});

				setInterval(function() { aiota.processHeartbeat(config.processName, config.serverName, aiotaDB); }, 10000);
			}
		});
	}
});