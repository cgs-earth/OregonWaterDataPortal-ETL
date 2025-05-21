This directory represents all code needed to generate the geometry data for the relevant counties for groundwater data. We need to do this since the client request for groundwater data was to only include 3 counties in Oregon. This is since there are thousands of wells and thus helps to reduce the size of the crawl.

We have to convert the geojson files to esrijson so that they can be used as a polygon filter in the esri api. The esri api does not support geojson polygon filters.

To generate the esrijson files, run 

`npm i; node geojsonToEsrijson.js`

## File Explanation

`relevant_locations.geojson` is the geojson of the 3 relevant counties
`relevant_locations_simple.geojson` is a simplified version of `relevant_locations.geojson` with reduced precision since the API does not support geojson with high precision
`relevant_locations_simple.esri.json` is the esrijson of `relevant_locations_simple.geojson`