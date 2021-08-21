# Configuration expectations

## Mysql is database

+ MAX_CONTENT_LENGTH = 3*1024*1924

## As we have Chinese in the database and coding so set 

+ JSON_AS_ASCII= False

## Opening DEBUG mode
```
class DevlopConfig(Config):
  DEBUG = True
```

# Running command

## python3 app.py runserver -p port_num



