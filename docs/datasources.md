New Datasource Request on May 14th

(2 and 3 need to be joined together, 2 provides the features and 3 the timeseries data)

1. List of wells from aqwms (we have only done surface level monitoring so far)
2. ground water well logs (this is only the feature and the geometry)
   - https://arcgis.wrd.state.or.us/arcgis/rest/services/dynamic/wl_well_logs_qry_WGS84/MapServer/0
   - record of the approx location of a new well - `type_of_log = W` `work_new = 1`
     - `W` represents the digging of a well and `work_new` signifies it is the first time it was dug
   - we ideally want the geometry to be a circle centered on `longitude_dec`, `latitude_dec` with radius (feet) `est_horizontal_error`
3. https://apps.wrd.state.or.us/apps/gw/gw_data_rws/api/WASC0002695/gw_measured_water_level/?start_date=1/1/1905&end_date=12/30/2050&public_viewable=
   - /api/{well_id_from_mapserver_in_step_2}/{variable}/?start_date={start_date}&end_date={end_date}&public_viewable=
   - (from MapServer) wl_county_code + wl_nbr (7 digits w/ leading 0s) - vast majority of features in the mapserver may not have the ground water data
   - i.e. WASC is the county code, 2695 is the well number, and 3 padding 0s - crawl this on a schedule
   - documented at https://apps.wrd.state.or.us/apps/gw/gw_info/gw_hydrograph/Hydrograph.aspx?gw_logid=WASC0002695
