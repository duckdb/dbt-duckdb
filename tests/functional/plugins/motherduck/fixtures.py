#
# Models
#

models__gen_data_macro = """
select * from {{ ref("seed") }}
"""

#
# Macros
#

macros__generate_database_name = """
{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {{ target.database | trim }}_{{ var("build_env") | trim }}_{{ var("org_prefix") | trim }}
{%- endmacro %}
"""


macros__generate_schema_name = """
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {{ target.schema | trim }}_{{ var("build_env") | trim }}_{{ var("org_prefix") | trim }}
{%- endmacro %}
"""

#
# Seeds
#

seeds__example_seed_csv = """a,b,c
1,2,3
4,5,6
7,8,9
"""
