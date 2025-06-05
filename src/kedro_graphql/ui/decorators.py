from importlib import import_module
from kedro_graphql.logs.logger import logger
from collections import defaultdict

UI_PLUGINS = {"FORMS": defaultdict(list),
              "DATA": defaultdict(list),
              "DASHBOARD": defaultdict(list),
              }


def discover_plugins(config):
    """Discover and import plugins based on the configuration.
    Args:
        config (dict): Configuration dictionary containing the imports.
    """
    imports = [i.strip()
               for i in config["KEDRO_GRAPHQL_IMPORTS"].split(",") if len(i.strip()) > 0]
    for i in imports:
        import_module(i)


def ui_form(pipeline):
    """Register a UI form plugin for a specific pipeline.

    Args:
        pipeline (str): Name of the pipeline for which the form is registered.
    """

    def register_plugin(plugin_class):
        UI_PLUGINS["FORMS"][pipeline].append(plugin_class)
        logger.info("registered ui_form plugin: " + str(plugin_class))
        return plugin_class

    return register_plugin


def ui_data(pipeline):
    """Register a UI data plugin for a specific pipeline.

    Args:
        pipeline (str): Name of the pipeline for which the data plugin is registered.
    """

    def register_plugin(plugin_class):
        UI_PLUGINS["DATA"][pipeline].append(plugin_class)
        logger.info("registered ui_data plugin: " + str(plugin_class))
        return plugin_class

    return register_plugin


def ui_dashboard(pipeline):
    """Register a UI dashboard plugin for a specific pipeline.

    Args:
        pipeline (str): Name of the pipeline for which the dashboard plugin is registered.
    """

    def register_plugin(plugin_class):
        UI_PLUGINS["DASHBOARD"][pipeline].append(plugin_class)
        logger.info("registered ui_dashboard plugin: " + str(plugin_class))
        return plugin_class

    return register_plugin
