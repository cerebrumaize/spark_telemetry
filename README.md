# kick off services
> docker compose up -d

# day 1
delete a manual msg from the 2nd partition. But in prod env, none msg should be deleted.
Will add DLQ and schema registry to avoid delete bad msg.