{% macro create_external_tables(
    stage_database='CT_BHUVAN_JAMBHULKAR_DB',
    stage_schema='CAPSTONE_BHUVAN',
    stage_name='CAPSTONE_DB',
    target_schema='CAPSTONE_BHUVAN',
    file_format='JSON_FF'
) %}
 
    {% set stage =
        stage_database ~ '.' ~ stage_schema ~ '.' ~ stage_name
    %}
 
    {% set format =
        stage_database ~ '.' ~ stage_schema ~ '.' ~ file_format
    %}
 
    {# Get all files from the stage #}
 
    {% set list_sql %}
        LIST @{{ stage }}
    {% endset %}
 
    {% set results = run_query(list_sql) %}
 
    {% if execute %}
 
        {% set folders = [] %}
 
        {# Discover unique parent folders of JSON files #}
 
        {% for row in results.rows %}
 
            {% set file_path = row[0] | string %}
            {% set file_path_lower = file_path | lower %}
 
            {% if file_path_lower[-5:] == '.json' %}
 
                {% set path_parts = file_path.split('/') %}
 
                {% if path_parts | length >= 2 %}
 
                    {% set folder = path_parts[-2] %}
 
                    {% if folder not in folders %}
                        {% do folders.append(folder) %}
                    {% endif %}
 
                {% endif %}
 
            {% endif %}
 
        {% endfor %}
 
        {# Create one external table per discovered folder #}
 
        {% for folder in folders %}
 
            {% set table_name =
                folder
                | replace('-', '_')
                | replace(' ', '_')
                | upper
            %}
 
            {% set sql %}
 
                CREATE OR REPLACE EXTERNAL TABLE
                    {{ stage_database }}.{{ target_schema }}.EXT_{{ table_name }}
 
                WITH LOCATION =
                    @{{ stage }}
 
                PATTERN = '.*{{ folder }}/.*[.]json'
 
                FILE_FORMAT = (
                    FORMAT_NAME = '{{ format }}'
                )
 
                AUTO_REFRESH = FALSE
 
            {% endset %}
 
            {% do run_query(sql) %}
 
            {{ log(
                "Created external table EXT_" ~ table_name
                ~ " for folder " ~ folder,
                info=True
            ) }}
 
        {% endfor %}
 
        {{ log(
            "Discovered " ~ (folders | length)
            ~ " source folders.",
            info=True
        ) }}
 
    {% endif %}
 
{% endmacro %}