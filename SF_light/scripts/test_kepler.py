from keplergl import KeplerGl 
import json
map_1 = KeplerGl(height=500)
df = json.load(open("/Users/Timothe/Downloads/SF_light/output_files/costs_neighbors.json", "r"))
map_1.add_data(data=df, name="data")
map_1.save_to_html(file_name="my_keplergl_map.html")
