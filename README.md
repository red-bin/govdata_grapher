# govdata_grapher
Throws some ETL at a csv and creates visualizations. Todo tomorrow: write this readme with images.

Bokeh interface inferred through below
1. Infers which columns are: latitude, longitude, timestamps,
2. Type checking happens in postgres through sampling.
4. Discovers shared keys based on column name.
5. Works decently well with bunches of csvs.

Needs work:
1. Shared keys should be determined without much, if any, knowledge about the header.
2. Per-project databases need to be turned into schemas.

Design Goals:
1. Everything should be preserved into postgres and avoid pandas where possible.
2. It should be trivial to infer columns out of 
Across an arbitrary set of .csvs, in batch, automatically discover: "primary" and "foreign" key columns, addresses columns (even

Oddities:
Only works with Chicago dataset so far.

### Path forward

Currently creates graphs like: 
https://viz.mchap.io/landmarks_viz
