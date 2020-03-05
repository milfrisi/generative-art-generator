INSERT INTO TABLE ${db}.uuids
SELECT java_method("java.util.UUID", "randomUUID")
