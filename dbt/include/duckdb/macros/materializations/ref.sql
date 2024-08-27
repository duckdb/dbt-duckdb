{% macro ref() %}
    -- default ref: https://docs.getdbt.com/reference/dbt-jinja-functions/builtins
    -- extract user-provided positional and keyword arguments
    {% set version = kwargs.get('version') or kwargs.get('v') %}
    {% set packagename = none %}
    {%- if (varargs | length) == 1 -%}
        {% set modelname = varargs[0] %}
    {%- else -%}
        {% set packagename = varargs[0] %}
        {% set modelname = varargs[1] %}
    {% endif %}

    -- call builtins.ref based on provided positional arguments
    {% set rel = None %}
    {% if packagename is not none %}
        {% set rel = builtins.ref(packagename, modelname, version=version) %}
    {% else %}
        {% set rel = builtins.ref(modelname, version=version) %}
    {% endif %}

    {% if execute %}
        {% if graph.get('nodes') %}
            {% for node in graph.nodes.values() | selectattr("name", "equalto", modelname) %}
                -- Get the associated materialization from the node config
                {% set materialization = node.config.materialized %}
                -- Get the associated plugin from the node config
                {% set plugin = node.config.plugin %}

                {% if plugin == 'unity' and materialization == 'external_table' %}
                    -- Retrieve the catalog value from the active target configuration
                    {% set catalog = target.get("catalog", "unity") %}
                    -- Get the associated schema from the node config
                    {% set schema = node.config.schema %}

                    {% if not schema %}
                        {% set schema = 'default' %}
                    {% endif %}

                    {% set new_rel = catalog ~ '.' ~ schema ~ '.' ~ rel.identifier %}

                    {% do return(new_rel) %}
                {% else %}
                    {% do return(rel) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}

    -- return the original relation object by default
    {% do return(rel) %}
{% endmacro %}
