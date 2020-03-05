CREATE EXTERNAL TABLE {{ db }}.uuids (
    uuid STRING
)
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/uuids';
