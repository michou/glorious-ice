import datetime

from peewee import Model, CharField, SqliteDatabase, ForeignKeyField, DateTimeField

external_database = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = external_database


class Archive(BaseModel):
    name = CharField()
    folder_name = CharField(index=True)
    checksum = CharField()
    created_at = DateTimeField(default=datetime.datetime.now())
    uploaded_at = DateTimeField(null=True)
    external_id = CharField(null=True)


class FileEntry(BaseModel):
    archive = ForeignKeyField(Archive, backref='contents')
    path = CharField()
