{#
    This is just an example that you can use Jinja2 templating syntax
    in your Hive scripts to have conditional statements and variables
    that are evalutated before the code gets deployed or exported.
#}
SELECT *
FROM ${db}.uuids
{% if smallDataset == 'true' %}
WHERE uuid LIKE 'a%'
LIMIT 10
{% endif %}
