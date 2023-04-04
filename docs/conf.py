"""
This file is the configuration file for the Sphinx documentation builder.
See the documentation: http://www.sphinx-doc.org/en/master/config
"""

import os
import pathlib
import sys

# Doc Path
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

project = "Determined"
html_title = "Determined AI Documentation"
copyright = "2023, Determined AI"
author = "hello@determined.ai"
version = pathlib.Path(__file__).parents[1].joinpath("VERSION").read_text()
release = version
language = "en"

source_suffix = {".rst": "restructuredtext"}
templates_path = ["_templates"]
html_static_path = ["assets"]
html_css_files = [
    "https://cdn.jsdelivr.net/npm/@docsearch/css@3",
    "styles/determined.css",
]

html_js_files = [
    ("https://cdn.jsdelivr.net/npm/@docsearch/js@3", {"defer": "defer"}),
    ("scripts/docsearch.sbt.js", {"defer": "defer"}),
]


def env_get_outdated(app, env, added, changed, removed):
    return ["index"]


def setup(app):
    app.connect("env-get-outdated", env_get_outdated)


exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "examples",
    "requirements.txt",
    "site",
    "README.md",
    "release-notes/README.md",
]
html_baseurl = "https://docs.determined.ai"  # Base URL for sitemap.
highlight_language = "none"
todo_include_todos = True

# HTML theme settings
html_show_sourcelink = False
html_show_sphinx = False
html_theme = "sphinx_book_theme"
html_favicon = "assets/images/favicon.ico"
html_last_updated_fmt = None
# See https://pradyunsg.me/furo/

html_sidebars = {
    "**": [
        "navbar-logo.html",
        "sidebar-version.html",
        "search-field.html",
        "sbt-sidebar-nav.html",
    ]
}

pygments_style = "sphinx"
pygments_dark_style = "monokai"
html_theme_options = {
    "logo": {
        "image_light": "assets/images/logo-determined-ai.svg",
        "image_dark": "assets/images/logo-determined-ai-white.svg",
    },
    "repository_url": "https://github.com/determined-ai/determined",
    "use_repository_button": True,
    "use_download_button": False,
    "use_fullscreen_button": False,
}
html_use_index = True
html_domain_indices = True

extensions = [
    "sphinx_ext_downloads",
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinx_sitemap",
    "sphinx_reredirects",
    "myst_parser",
]

myst_extensions = [
    "colon_fence",
]

# Our custom sphinx extension uses this value to decide where to look for
# downloadable files.
dai_downloads_root = os.path.join("site", "downloads")

# sphinx.ext.autodoc configurations.
# See https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html
autosummary_generate = True
autoclass_content = "class"
autodoc_mock_imports = [
    "mmcv",
    "mmdet",
    "transformers",
    "pytorch_lightning",
    "deepspeed",
    "datasets",
    "analytics",
]

# sphinx-sitemap configurations.
# See https://github.com/jdillard/sphinx-sitemap
# The URLs generated by sphinx-sitemap include both the version number and the
# language by default. We don't use language in the published URL, and we also
# want to encourage the latest version of the docs to be indexed, so only
# include that variant in the sitemap.
sitemap_url_scheme = "latest/{link}"

redirects = {
    "api-pytorch": "training/apis-howto/api-pytorch-ug.html",
    "apis-howto/api-core/checkpoints": "../../training/apis-howto/api-core-ug.html",
    "apis-howto/api-core/hpsearch": "../../training/apis-howto/api-core-ug.html",
    "apis-howto/api-core/overview": "../../training/apis-howto/api-core-ug.html",
    "apis-howto/api-core/metrics": "../../training/apis-howto/api-core-ug.html",
    "apis-howto/api-core/getting-started": "../../training/apis-howto/api-core-ug.html",
    "apis-howto/api-core/distributed": "../../training/apis-howto/api-core-ug.html",
    "concepts/elastic-infrastructure": "../architecture/introduction.html",
    "concepts/index": "../architecture/index.html",
    "concepts/resource-pool": "../architecture/introduction.html",
    "concepts/scheduling": "../architecture/introduction.html",
    "concepts/yaml": "../architecture/introduction.html",
    "examples": "example-solutions/examples.html",
    "experiment-config": "reference/reference-training/experiment-config-reference.html",
    "features/command-notebook-config": "../architecture/introduction.html",
    "features/commands-and-shells": "../interfaces/commands-and-shells.html",
    "features/config-template": "../architecture/introduction.html",
    "features/elastic-infrastructure": "../architecture/introduction.html",
    "features/experiments": "../training/submit-experiment.html",
    "features/index": "../architecture/introduction.html",
    "features/job-queue": "../training/submit-experiment.html",
    "features/model-registry": "../training/model-management/model-registry-org.html",
    "features/notebooks": "../interfaces/notebooks.html",
    "features/resource-pool": "../architecture/introduction.html",
    "features/scheduling": "../architecture/introduction.html",
    "features/system-architecture": "../architecture/system-architecture.html",
    "features/tensorboard": "../architecture/introduction.html",
    "features/terminology-concepts": "../architecture/introduction.html",
    "features/yaml": "../architecture/introduction.html",
    "getting-started": "quickstart-mdldev.html",
    "how-to/custom-env": "../training/setup-guide/custom-env.html",
    "how-to/distributed-training": "../training/dtrain-introduction.html",
    "how-to/hyperparameter-tuning": "../training/hyperparameter/overview.html",
    "how-to/index": "../index.html",
    "how-to/installation/aws": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/install-on-aws.html",
    "how-to/installation/custom-pod-specs": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/custom-pod-specs.html",
    "how-to/installation/deploy": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/overview.html",
    "how-to/installation/docker": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/docker.html",
    "how-to/installation/dynamic-agents-aws": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/dynamic-agents-aws.html",
    "how-to/installation/dynamic-agents-gcp": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/dynamic-agents-gcp.html",
    "how-to/installation/gcp": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/install-gcp.html",
    "how-to/installation/k8s-dev-guide": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/k8s-dev-guide.html",
    "how-to/installation/kubernetes": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/install-on-kubernetes.html",
    "how-to/installation/linux-packages": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/linux-packages.html",
    "how-to/installation/network-requirements": "../../cluster-setup-guide/basic.html",
    "how-to/installation/requirements": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/requirements.html",
    "how-to/installation/setup-aks-cluster": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-aks-cluster.html",
    "how-to/installation/setup-eks-cluster": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-eks-cluster.html",
    "how-to/installation/setup-gke-cluster": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-gke-cluster.html",
    "how-to/installation/upgrades-troubleshoot": "../../cluster-setup-guide/upgrade.html",
    "how-to/install-cli": "../interfaces/cli-ug.html",
    "how-to/install-main": "../cluster-setup-guide/basic.html",
    "how-to/model-debug": "../training/debug-models.html",
    "how-to/notebooks": "../architecture/introduction.html",
    "how-to/profiling": "../training/dtrain-introduction.html",
    "how-to/rest-apis": "../reference/rest-api.html",
    "how-to/tensorboard": "../architecture/introduction.html",
    "how-to/use-trained-models": "../training/model-management/overview.html",
    "integrations/ecosystem-integration": "../integrations/ecosystem/ecosystem-integration.html",
    "integrations/prometheus": "../integrations/prometheus/prometheus",
    "interact/api-experimental-client": "../index.html",
    "interact/cli": "../interfaces/cli-ug.html",
    "interact/index": "../interfaces/commands-and-shells.html",
    "interact/rest-apis": "../reference/rest-api.html",
    "introduction": "../architecture/introduction.html",
    "join-community": "index.html",
    "model-hub/index": "../model-hub-library/index.html",
    "model-hub/mmdetection/api": "../../reference/reference-model-hub/modelhub/mmdetection-api.html",
    "model-hub/mmdetection/index": "../../model-hub-library/mmdetection/overview.html",
    "model-hub/transformers/api": "../../reference/reference-model-hub/modelhub/transformers-api.html",
    "model-hub/transformers/examples": "../../model-hub-library/transformers/examples.html",
    "model-hub/transformers/index": "../../model-hub-library/transformers/overview.html",
    "model-hub/transformers/tutorial": "../../model-hub-library/transformers/tutorial.html",
    "post-training/index": "../training/model-management/overview.html",
    "post-training/model-registry": "../training/model-management/model-registry-org.html",
    "post-training/use-trained-models": "../training/model-management/overview.html",
    "prepare-data/index": "../training/load-model-data.html",
    "prepare-environment/custom-env": "../training/setup-guide/custom-env.html",
    "prepare-environment/custom-pod-specs": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/custom-pod-specs.html",
    "prepare-environment/index": "../training/setup-guide/overview.html",
    "reference/api/api-determined": "../../training/apis-howto/overview.html",
    "reference/api/api-estimator": "../../training/apis-howto/api-estimator-ug.html",
    "reference/api/api-experimental-client": "../../index.html",
    "reference/api/api-keras": "../../training/apis-howto/api-keras-ug.html",
    "reference/api/api-pytorch-lightning": "../../training/apis-howto/api-pytorch-lightning-ug.html",
    "reference/api/api-pytorch-samplers": "../../training/apis-howto/api-pytorch-ug.html",
    "reference/api/api-pytorch": "../../training/apis-howto/api-pytorch-ug.html",
    "reference/api/api-trial_context": "../../training/apis-howto/overview.html",
    "reference/api/command-notebook-config": "../../architecture/introduction.html",
    "reference/api/config-template": "../../architecture/introduction.html",
    "reference/api/experiment-config": "../../reference/reference-training/experiment-config-reference.html",
    "reference/attributions": "../attributions.html",
    "reference/cli": "../reference/cli-reference.html",
    "reference/cluster/cluster-config": "../../reference/reference-deploy/index.html",
    "reference/cluster-config": "../reference/reference-deploy/index.html",
    "reference/cluster/helm-config": "../reference-deploy/config/helm-config-reference.html",
    "reference/cluster/historical-cluster-usage-data": "../../cluster-setup-guide/historical-cluster-usage-data.html",
    "reference/command-notebook-config": "../architecture/introduction.html",
    "reference/config-template": "../architecture/introduction.html",
    "reference/experiment-config": "../reference/reference-training/experiment-config-reference.html",
    "reference/helm-config": "../reference/reference-deploy/config/helm-config-reference.html",
    "reference/historical-cluster-usage-data": "../cluster-setup-guide/historical-cluster-usage-data.html",
    "reference/index": "../reference/python-sdk.html",
    "reference/python-api.html": "../reference/python-sdk.html",
    "sysadmin-basics/cluster-config": "../reference/reference-deploy/index.html",
    "sysadmin-basics/elasticsearch-logging-backend": "../cluster-setup-guide/elasticsearch-logging-backend.html",
    "sysadmin-basics/historical-cluster-usage-data": "../cluster-setup-guide/historical-cluster-usage-data.html",
    "sysadmin-basics/index": "../cluster-setup-guide/basic.html",
    "sysadmin-basics/network-requirements": "../cluster-setup-guide/basic.html",
    "sysadmin-basics/oauth": "../cluster-setup-guide/security/oauth.html",
    "sysadmin-basics/oidc": "../cluster-setup-guide/security/oidc.html",
    "sysadmin-basics/saml": "../cluster-setup-guide/security/saml.html",
    "sysadmin-basics/scim": "../cluster-setup-guide/security/scim.html",
    "sysadmin-basics/tls": "../cluster-setup-guide/security/tls.html",
    "sysadmin-basics/troubleshoot": "../cluster-setup-guide/troubleshooting.html",
    "sysadmin-basics/upgrades-troubleshoot": "../cluster-setup-guide/upgrade.html",
    "sysadmin-basics/upgrades": "../cluster-setup-guide/upgrade.html",
    "sysadmin-basics/users": "../cluster-setup-guide/users.html",
    "sysadmin-basics/workspaces": "../cluster-setup-guide/workspaces.html",
    "sysadmin-deploy-on-aws/aws-spot": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/aws-spot.html",
    "sysadmin-deploy-on-aws/dynamic-agents-aws": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/dynamic-agents-aws.html",
    "sysadmin-deploy-on-aws/index": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/overview.html",
    "sysadmin-deploy-on-aws/install-on-aws": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/install-on-aws.html",
    "sysadmin-deploy-on-gcp/dynamic-agents-gcp": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/dynamic-agents-gcp.html",
    "sysadmin-deploy-on-gcp/index": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/overview.html",
    "sysadmin-deploy-on-gcp/install-gcp": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/install-gcp.html",
    "sysadmin-deploy-on-gcp/install-on-gcp": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/install-gcp.html",
    "sysadmin-deploy-on-k8s/helm-config": "../reference/reference-deploy/config/helm-config-reference.html",
    "sysadmin-deploy-on-k8s/index": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/overview.html",
    "sysadmin-deploy-on-k8s/install-on-kubernetes": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/install-on-kubernetes.html",
    "sysadmin-deploy-on-k8s/k8s-dev-guide": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/k8s-dev-guide.html",
    "sysadmin-deploy-on-k8s/setup-aks-cluster": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-aks-cluster.html",
    "sysadmin-deploy-on-k8s/setup-eks-cluster": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-eks-cluster.html",
    "sysadmin-deploy-on-k8s/setup-gke-cluster": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/setup-gke-cluster.html",
    "sysadmin-deploy-on-prem/deploy": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/overview.html",
    "sysadmin-deploy-on-prem/docker": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/docker.html",
    "sysadmin-deploy-on-prem/index": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/overview.html",
    "sysadmin-deploy-on-prem/linux-packages": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/linux-packages.html",
    "sysadmin-deploy-on-prem/requirements": "../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-prem/requirements.html",
    "topic-guides/benefits-of-determined": "../architecture/introduction.html",
    "topic-guides/checkpoints": "../training/apis-howto/overview.html",
    "topic-guides/cluster-configuration/elasticsearch-logging-backend": "../../cluster-setup-guide/elasticsearch-logging-backend.html",
    "topic-guides/cluster-configuration/oauth": "../../cluster-setup-guide/security/oauth.html",
    "topic-guides/cluster-configuration/saml": "../../cluster-setup-guide/security/saml.html",
    "topic-guides/cluster-configuration/scim": "../../cluster-setup-guide/security/scim.html",
    "topic-guides/cluster-configuration/tls": "../../cluster-setup-guide/security/tls.html",
    "topic-guides/cluster-configuration/users": "../../cluster-setup-guide/users.html",
    "topic-guides/commands-and-shells": "../interfaces/commands-and-shells.html",
    "topic-guides/deployment/aws": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/overview.html",
    "topic-guides/deployment/aws-spot": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-aws/aws-spot.html",
    "topic-guides/deployment/determined-on-kubernetes": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-k8s/overview.html",
    "topic-guides/deployment/gcp": "../../cluster-setup-guide/deploy-cluster/sysadmin-deploy-on-gcp/overview.html",
    "topic-guides/distributed-training/effective-distributed-training": "../../training/dtrain-introduction.html",
    "topic-guides/distributed-training/optimizing-distributed-training": "../../training/dtrain-introduction.html",
    "topic-guides/hp-constraints-det": "../training/hyperparameter/hp-constraints-det.html",
    "topic-guides/hp-tuning-defined": "../training/hyperparameter/overview.html",
    "topic-guides/hp-tuning-det/hp-adaptive-asha": "../../training/hyperparameter/search-methods/hp-adaptive-asha.html",
    "topic-guides/hp-tuning-det/hp-constraints-det": "../../training/hyperparameter/hp-constraints-det.html",
    "topic-guides/hp-tuning-det/hp-grid": "../../training/hyperparameter/search-methods/hp-grid.html",
    "topic-guides/hp-tuning-det/hp-random": "../../training/hyperparameter/search-methods/hp-random.html",
    "topic-guides/hp-tuning-det/hp-single": "../../training/hyperparameter/search-methods/hp-single.html",
    "topic-guides/hp-tuning-det/hp-tuning-defined": "../../training/hyperparameter/overview.html",
    "topic-guides/hp-tuning-det/index": "../../training/hyperparameter/overview.html",
    "topic-guides/index": "../index.html",
    "topic-guides/model-definitions/best-practices": "../../training/apis-howto/overview.html",
    "topic-guides/model-definitions/index": "../../training/apis-howto/overview.html",
    "topic-guides/model-definitions/trial-api": "../../training/apis-howto/overview.html",
    "topic-guides/oauth": "../cluster-setup-guide/security/oauth.html",
    "topic-guides/saml": "../cluster-setup-guide/security/saml.html",
    "topic-guides/scim": "../cluster-setup-guide/security/scim.html",
    "topic-guides/system-concepts/elastic-infrastructure": "../../architecture/introduction.html",
    "topic-guides/system-concepts/resource-pool": "../../architecture/introduction.html",
    "topic-guides/system-concepts/scheduling": "../../architecture/introduction.html",
    "topic-guides/system-concepts/system-architecture": "../../architecture/index.html",
    "topic-guides/system-concepts/terminology-concepts": "../../architecture/introduction.html",
    "topic-guides/tls": "../cluster-setup-guide/security/tls.html",
    "topic-guides/training/experiment-lifecycle": "../../training/submit-experiment.html",
    "topic-guides/training/reproducibility": "../../training/dtrain-introduction.html",
    "topic-guides/training/training-units": "../../reference/reference-training/experiment-config-reference.html",
    "topic-guides/user-interfaces": "../interfaces/commands-and-shells.html",
    "topic-guides/users": "../cluster-setup-guide/users.html",
    "topic-guides/yaml": "../architecture/introduction.html",
    "training-apis/api-core/checkpoints": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/distributed": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/getting-started": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/hpsearch": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/index": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/metrics": "../../training/apis-howto/api-core-ug.html",
    "training-apis/api-core/reference": "../../reference/reference-training/training/api-core-reference.html",
    "training-apis/api-determined": "../index.html",
    "training-apis/api-estimator-reference": "../reference/reference-training/training/api-estimator-reference.html",
    "training-apis/api-estimator": "../training/apis-howto/api-estimator-ug.html",
    "training-apis/api-keras-reference": "../reference/reference-training/training/api-keras-reference.html",
    "training-apis/api-keras": "../training/apis-howto/api-keras-ug.html",
    "training-apis/api-pytorch-advanced": "../training/apis-howto/api-pytorch-ug.html",
    "training-apis/api-pytorch-lightning": "../training/apis-howto/api-pytorch-lightning-ug.html",
    "training-apis/api-pytorch-porting": "../tutorials/pytorch-porting-tutorial.html",
    "training-apis/api-pytorch-reference": "../reference/reference-training/training/api-pytorch-reference.html",
    "training-apis/api-pytorch-samplers": "../training/apis-howto/api-pytorch-ug.html",
    "training-apis/api-pytorch": "../training/apis-howto/api-pytorch-ug.html",
    "training-apis/api-trial_context": "../index.html",
    "training-apis/best-practices-for-model-definitions": "../index.html",
    "training-apis/best-practices": "../index.html",
    "training-apis/examples": "../example-solutions/examples.html",
    "training-apis/experiment-config": "../reference/reference-training/experiment-config-reference.html",
    "training-apis/faq": "../index.html",
    "training-apis/index": "../training/apis-howto/overview.html",
    "training-apis/save-checkpoints": "../reference/reference-training/experiment-config-reference.html",
    "training-apis/training-units": "../reference/reference-training/experiment-config-reference.html",
    "training-apis/trial-api": "../index.html",
    "training-debug/index": "../training/debug-models.html",
    "training-debug/profiling": "../training/dtrain-introduction.html",
    "training-distributed/effective-distributed-training": "../training/dtrain-introduction.html",
    "training-distributed/index": "../training/dtrain-introduction.html",
    "training-hyperparameter/hp-adaptive-asha": "../training/hyperparameter/search-methods/hp-adaptive-asha.html",
    "training-hyperparameter/hp-constraints-det": "../training/hyperparameter/hp-constraints-det.html",
    "training-hyperparameter/hp-grid": "../training/hyperparameter/search-methods/hp-grid.html",
    "training-hyperparameter/hp-random": "../training/hyperparameter/search-methods/hp-random.html",
    "training-hyperparameter/hp-single": "../training/hyperparameter/search-methods/hp-single.html",
    "training-hyperparameter/hp-tuning-defined": "../training/hyperparameter/overview.html",
    "training-hyperparameter/index": "../training/hyperparameter/overview.html",
    "training-reproducibility/index": "../training/dtrain-introduction.html",
    "training-run/index": "../training/submit-experiment.html",
    "tutorials/data-access": "../training/load-model-data.html",
    "tutorials/model-registry": "../training/model-management/model-registry-org.html",
    "tutorials/notebook-tutorial": "../interfaces/notebooks.html",
    "tutorials/porting-tutorial": "../tutorials/pytorch-porting-tutorial.html",
    "tutorials/quick-start": "../quickstart-mdldev.html",
}
