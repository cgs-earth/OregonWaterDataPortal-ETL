// This script converts a GeoJSON file to an ESRI JSON file
// this is needed sine the esri API only accepts ESRI JSON
// this is in js and not python since we don't need to call this
// at runtime; we just need to get the static file since it is just county borders

const { geojsonToArcGIS } = require("@terraformer/arcgis");
const fs = require("fs");

// Read the GeoJSON file as a string and parse it
const geojsonData = fs.readFileSync(
  "./relevant_locations_simple.geojson",
  "utf8",
);
const geojson = JSON.parse(geojsonData);

// Convert to ESRI JSON
const esrijson = geojsonToArcGIS(geojson);

// Write to output file
fs.writeFileSync(
  "relevant_locations_simple.esri.json",
  JSON.stringify(esrijson, null, 2),
);
