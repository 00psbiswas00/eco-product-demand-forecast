from scripts import fetch_trends,process_trends, openfoodfacts_ingest, process_products, map_sustainability_tags

#ingest the data
fetch_trends.main()
openfoodfacts_ingest.main()

#Process the data
process_trends.main()
process_products.main()
map_sustainability_tags.main()