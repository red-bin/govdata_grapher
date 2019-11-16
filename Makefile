setup_view: create_cache
	#psql -d$(PROJECT_NAME) -U$(PROJECT_NAME) -v project_name=$(PROJECT_NAME) < sql/setup_view.sql
	./create_materialized_view.py

create_cache: create_indices
	./create_cache.py

create_indices: create_csv
	./create_indices.py

create_csv: setup_geo
	./create_csv.py

setup_geo: add_date_cols
	psql -d$(PROJECT_NAME) -U$(PROJECT_NAME) < sql/setup_extensions.sql
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/grid_canvas_cropped.geojson" -nln grid_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/Boundaries - Central Business District.geojson" -nln cbd_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/Boundaries - Wards (2015-).geojson" -nln wards2015_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/Boundaries - Wards (2003-2015).geojson" -nln wards2003_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/Boundaries - Census Tracts - 2010.geojson" -nln census_tracts_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=$(PROJECT_NAME) host=localhost user=$(PROJECT_NAME) password=$(PROJECT_NAME)" "data/neighborhoods.geojson" -nln neighborhood_geo -overwrite
	./setup_geo.py

add_date_cols: data.conf
	./add_date_field_cols.py

data.conf: populate_geocode_tables
	./create_data_conf.py

populate_geocode_tables: sql_clean_proj
	./populate_geocoding_tables.py

sql_clean_proj: project_table
	cat sql/clean_raw.sql | sudo -u postgres psql -d $(PROJECT_NAME) -U$(PROJECT_NAME) -v table_name=$(PROJECT_NAME)

project_table: geocoding_corrections 
	./create_and_populate_project_table.py

geocoding_corrections: sql_clean_raw
	cat sql/geocoding_corrections.sql | sudo -u postgres psql -d $(PROJECT_NAME) -U$(PROJECT_NAME)

sql_clean_raw: load_raw_data 
	cat sql/clean_raw.sql | sudo -u postgres psql -d $(PROJECT_NAME) -U$(PROJECT_NAME) -v table_name='raw_14418__p518853_chapman_search_warrant'

load_raw_data: data_file
	./load_raw_data_dir.py

data_file: setup_database
	ORIG_DATA_PATH=`jq -r '.original_data_path' project.conf`; \
	DATA_PATH=`jq -r '.data_path' project.conf`; \
	DATA_DIR=`jq -r '.data_dir' project.conf`; \
	mkdir -p $$DATA_DIR/dropdowns ; \
	cp -p $$ORIG_DATA_PATH $$DATA_PATH

setup_database: project.conf
	cat sql/clean.sql | sudo -u postgres psql -v project_name=$(PROJECT_NAME)

project.conf: 
	sudo service postgresql restart
	rm project.conf || :
	./generate_base_config.py
