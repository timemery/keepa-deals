import json
with open('/home/timscripts/keepa_api/keepa-deals/field_mapping.json') as file:
    try:
        json.load(file)
        print("JSON is valid")
    except json.JSONDecodeError as e:
        print(f"JSON error: {e}")