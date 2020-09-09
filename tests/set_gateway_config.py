import requests
from ruamel import yaml

# TODO: change this branch to master when gateway stuff is merged
r = requests.get(
    "https://raw.githubusercontent.com/RhodiumGroup/helm-chart/gateway/jupyter-config.yml"
)

data = yaml.safe_load(r.content)["dask-gateway"]["gateway"]["extraConfig"][
    "optionHandler"
]

# change float cores to int cores b/c float not allowed for local cluster

data = "from math import ceil\n" + data
data = data.replace(
    "scaling_factors[options.profile] * standard_cores",
    "ceil(scaling_factors[options.profile] * standard_cores)",
)

with open("dask_gateway_config.py", "w") as f:
    f.write(data)
