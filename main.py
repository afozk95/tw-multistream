from utils import read_json
from listener import Credential, MultiListener


with open("turkish_names.txt", "r") as f:
    raw = f.read()

names = raw.split("\n")

creds_json_path = "tw_api_creds.json"
creds_json = read_json(creds_json_path)
creds = [Credential(**obj) for obj in creds_json.values()]

listener = MultiListener(creds)
listener.filter(parameter_name="track", track=names, languages="tr")
