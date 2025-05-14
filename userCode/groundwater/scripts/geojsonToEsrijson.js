const { geojsonToArcGIS } = require("@terraformer/arcgis")
const fs = require("fs")

// Read the GeoJSON file as a string and parse it
const geojsonData = fs.readFileSync("./relevant_locations_simple.geojson", "utf8")
const geojson = JSON.parse(geojsonData)

// Convert to ESRI JSON
const esrijson = geojsonToArcGIS(geojson)

// Write to output file
fs.writeFileSync("relevant_locations_simple.esrijson", JSON.stringify(esrijson, null, 2))
