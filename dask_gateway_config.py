from math import ceil
from dask_gateway_server.options import Options, String, Select, Mapping


def cluster_options(user):

    # A mapping from profile name to configuration overrides
    standard_cores = 1.94
    standard_mem = 12.7
    scaling_factors = {"micro": 0.5, "standard": 1, "big": 2, "giant": 4}

    default_tolerations = {
        "0": [
            {
                "key": "k8s.dask.org_dedicated",
                "operator": "Equal",
                "value": "worker",
                "effect": "NoSchedule",
            }
        ]
    }

    def option_handler(options):
        if (":" not in options.worker_image) or (":" not in options.scheduler_image):
            raise ValueError("When specifying an image you must also provide a tag")
        extra_annotations = {"hub.jupyter.org/username": user.name}
        extra_labels = {
            "hub.jupyter.org/username": user.name,
            **options.extra_worker_labels,
        }

        this_env = options.env_items

        # add extra pip packages to be picked up by prepare.sh
        if options.extra_pip_packages != "":
            this_env["EXTRA_PIP_PACKAGES"] = options.extra_pip_packages

        # add gcsfuse Tokens
        this_env["GCSFUSE_TOKENS"] = options.gcsfuse_tokens

        # add google cloud auth
        this_env[
            "GOOGLE_APPLICATION_CREDENTIALS"
        ] = f"/opt/gcsfuse_tokens/{options.cred_name}.json"

        # pod config
        extra_pod_config = {
            "imagePullPolicy": "Always",
            "tolerations": list(options.worker_tolerations.values()),
        }

        return {
            "worker_cores": ceil(scaling_factors[options.profile] * standard_cores),
            "worker_memory": f"{scaling_factors[options.profile] * standard_mem:.2f}G",
            # setting images separately here to get a light-weight scheduler
            "worker_extra_container_config": {"image": options.worker_image},
            "scheduler_extra_container_config": {"image": options.scheduler_image},
            "scheduler_extra_pod_annotations": extra_annotations,
            "worker_extra_pod_annotations": extra_annotations,
            "worker_extra_pod_config": extra_pod_config,
            "scheduler_extra_pod_labels": extra_labels,
            "worker_extra_pod_labels": extra_labels,
            "environment": this_env,
        }

    return Options(
        Select(
            "profile",
            ["micro", "standard", "big", "giant"],
            default="standard",
            label="Cluster Profile",
        ),
        String("worker_image", default="rhodium/worker:gateway", label="Worker Image"),
        String(
            "scheduler_image",
            default="rhodium/scheduler:gateway",
            label="Scheduler Image",
        ),
        String("extra_pip_packages", default="", label="Extra pip Packages"),
        String("gcsfuse_tokens", default="", label="GCSFUSE Tokens"),
        String("cred_name", default="rhg-data", label="Bucket for Google Cloud Creds"),
        Mapping(
            "worker_tolerations",
            default=default_tolerations,
            label="Worker Pod Tolerations",
        ),
        Mapping("extra_worker_labels", default={}, label="Extra Worker Pod Labels"),
        Mapping("env_items", default={}, label="Environment Variables"),
        handler=option_handler,
    )


c.Backend.cluster_options = cluster_options
