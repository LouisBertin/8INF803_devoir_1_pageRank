
// Import MongoDB 
const MongoClient = require("mongodb").MongoClient;

// MongoDB localhost url
const url = "mongodb://localhost:27017/";

// Database name
const dbName = "test";

// Create graph
const graph = [
  { _id: "A", value: { linkTo: ["B", "C"], rank: 1 } },
  { _id: "B", value: { linkTo: ["C"], rank: 1 } },
  { _id: "C", value: { linkTo: ["A"], rank: 1 } },
  { _id: "D", value: { linkTo: ["C"], rank: 1 } }
];

// Connect MongoDB
MongoClient.connect(
	url,
	{ useNewUrlParser: true },
	function(err, db) {
		// Throw Err if can't connect
		if (err) throw err;
		// Connect to database
		var dataBase = db.db(dbName);
		var collection = dataBase.collection("graph");
		collection.deleteMany();
		collection.removeMany(); //Efface tout (sync)
		
		
		// Insert collection in mongoDB and use a promise to perform mapReduce
		collection.insertMany(graph, { w: 1 }).then(function(result) {

			// Map Function (use object "this"
			var map = function() {
				var linkTo = this.value.linkTo;
				var point = this._id;
				for (i = 0; i < linkTo.length; i++) {
					emit(linkTo[i], this.value.rank / linkTo.length);
				}
				emit(point, linkTo);
				emit(point, 0);
			};

			// Reduce Function
			var reduce = function(key, values) {
			
				var pageRank = 0.0;
				var linkTo = [];
				
				// DampingFactor gived in the exercice
				const DAMPING_FACTOR = 0.85;
				
				for (i = 0; i < values.length; i++) {
					if (values[i] instanceof Array) {
						// If the value is an array type (adjacency matrix), it's used to insert in MongoDB
						linkTo = values[i]; 
					} else {
						// Else, it's the calculed pageRank
						pageRank += values[i]; 
					}
				}
				
				// New pageRank with the DampingFactor
				pageRank = 1 - DAMPING_FACTOR + DAMPING_FACTOR * pageRank;
				
				// Variable used in debug
				var result = { linkTo: linkTo, rank: pageRank };
				
				return result;
			};

			// Iterate the sequence
			function iterate(i) {
				collection.mapReduce(map, reduce, {out: {replace: "graph"}},
					function(err, result) {
						// Throw error if error
						if (err) throw err;
						// Get data from databases to print progress
						collection
							.find()
							.toArray()
							.then(function(data) {
								console.log("Iterate : " + i);
								console.log(data);
								if (i < 20) {
									iterate(i + 1);
								} else {
									console.log("End of programm");
									db.close();
								}
							}
						);
					}
				);
			}
      iterate(0);
    });
  }
);
