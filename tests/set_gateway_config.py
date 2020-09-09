from ruamel import yaml
import requests

# TODO: change this branch to master when gateway stuff is merged
r = requests.get("https://raw.githubusercontent.com/RhodiumGroup/helm-chart/gateway/jupyter-config.yml")

data = yaml.safe_load(r.content)["dask-gateway"]["gateway"]["extraConfig"]["optionHandler"]

with open("dask_gateway_config.py", "w") as f:
    f.write(data)