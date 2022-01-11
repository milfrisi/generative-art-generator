CREATE EXTERNAL TABLE {{ db }}.ticker (
    timestamp STRING
)
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/ticker';
