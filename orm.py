import asyncio
import aiomysql
from aiomysql import Error


loop = asyncio.get_event_loop()


async def connect():
    try:
        conn = await aiomysql.connect(user='root', db='crawler', loop=loop)
    except aiomysql.Error as err:
        print(err)
    return conn


class Field:
    def __init__(self, f_type, required=True, default=None):
        self.f_type = f_type
        self.required = required
        self.default = default

    def validate(self, value):
        if value is None:
            if self.default is not None:
                return self.f_type(self.default)
            if not self.required:
                return None
            else:
                raise ValueError('field value is none but required')
        return self.f_type(value)


class IntField(Field):
    def __init__(self, required=True, default=None,
                 pri_key=False, auto_inc=False, bool=False):
        self.pri_key = pri_key
        self.auto_inc = auto_inc
        self.bool = bool
        super().__init__(int, required, default)

    def validate(self, value):
        if value is None and self.pri_key:
            return None
        return super().validate(value)

    def column_type(self):
        if self.bool:
            col_type = ['INT(1)']
        else:
            col_type = ['INT']
        if self.required:
            col_type.append('NOT NULL')
        if self.default is not None:
            col_type.append(f'DEFAULT {self.default}')
        if self.pri_key:
            col_type.append('PRIMARY KEY')
        if self.auto_inc:
            col_type.append('AUTO_INCREMENT')
        return ' '.join(col_type)


class StringField(Field):
    def __init__(self, size, required=True, default=None):
        self.size = size
        super().__init__(str, required, default)

    def validate(self, value):
        if len(value) > self.size:
            raise ValueError('incorrrect str size')
        return super().validate(value)

    def column_type(self):
        col_type = [f'VARCHAR({self.size})']
        if self.required:
            col_type.append('NOT NULL')
        if self.default is not None:
            col_type.append(f'DEFAULT {self.default}')
        return ' '.join(col_type)


class DatetimeField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, default)

    def column_type(self):
        col_type = ['DATETIME']
        if self.required:
            col_type.append('NOT NULL')
        if self.default is not None:
            col_type.append(f'DEFAULT {self.default}')
        return ' '.join(col_type)


class ModelMeta(type):
    def __new__(mcs, name, bases, namespace):
        if name == 'Model':
            return super().__new__(mcs, name, bases, namespace)

        meta = namespace.get('Meta')
        if meta is None:
            raise ValueError('Meta is none')
        if not hasattr(meta, 'table_name'):
            raise ValueError('table_name is empty')

        for base in bases:
            if hasattr(base, '_fields'):
                for k, v in base._fields.items():
                    namespace[k] = v

        fields = {k: v for k, v in namespace.items()
                  if isinstance(v, Field)}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name
        return super().__new__(mcs, name, bases, namespace)


class DoesNotExist(Exception):
    def __init__(self, message, errors=None):
        super().__init__(f'{message} does not exist')
        self.errors = errors


class Manage:
    def __init__(self):
        self.model_cls = None
        self.conn = loop.run_until_complete(connect())

    def __del__(self):
        self.conn.close()

    def __get__(self, instance, owner):
        self.model_cls = owner
        return self

    async def create_table(self):
        columns = []
        for name, field in self.model_cls._fields.items():
            columns.append(f'{name} {field.column_type()}')
        query = f'CREATE TABLE {self.model_cls._table_name} ' \
                f'({", ".join(columns)})'
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query)
        except aiomysql.Error as err:
            print(err)
        await cursor.close()

    async def create(self, **kwargs):
        columns = []
        values_str = []
        values_list = []
        for column, value in kwargs.items():
            if value is not None:
                columns.append(str(column))
                values_str.append('%s')
                values_list.append(value)
        query = f'INSERT INTO {self.model_cls._table_name} ' \
                f'({", ".join(columns)}) VALUES ({", ".join(values_str)})'
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, tuple(values_list))
        except aiomysql.Error as err:
            await cursor.close()
            raise ValueError(str(err.args[1]))
        await cursor.close()

        if 'id' in self.model_cls.__dict__ and 'id' not in kwargs:
            cursor = await self.conn.cursor()
            await cursor.execute('SELECT LAST_INSERT_ID()')
            field_names = [column[0] for column in cursor.description]
            for tuple_arg in await cursor.fetchall():
                kwargs['id'] = tuple_arg[0]
        await cursor.close()
        await self.conn.commit()
        return self.model_cls(**kwargs)

    async def update(self, *what, **kwargs):
        set_str = []
        set_list = []
        where_str = []
        where_list = []
        for column, value in kwargs.items():
            if column in what:
                set_str.append(f'{column} = %s')
                set_list.append(value)
            else:
                where_str.append(f'{column} = %s')
                where_list.append(value)
        query = f'UPDATE {self.model_cls._table_name} ' \
                f'SET {", ".join(set_str)} WHERE {" AND ".join(where_str)}'
        cursor = await self.conn.cursor()
        try:
            tuple
            await cursor.execute(query, (*set_list, *where_list))
        except aiomysql.Error as err:
            await cursor.close()
            raise ValueError(str(err.args[1]))
        await cursor.close()
        await self.conn.commit()

    async def all(self):
        query = f'SELECT * FROM {self.model_cls._table_name}'
        cursor = await self.conn.cursor()
        await cursor.execute(query)
        users_list = []
        field_names = [column[0] for column in cursor.description]
        for tuple_arg in await cursor.fetchall():
            kwarg = {}
            for idx, field_name in enumerate(field_names):
                kwarg[field_name] = tuple_arg[idx]
            users_list.append(self.model_cls(**kwarg))
        await cursor.close()
        return users_list

    async def get(self, **kwargs):
        values_str = []
        values_list = []
        for column, value in kwargs.items():
            values_str.append(f'{column} = %s')
            values_list.append(value)
        query = f'SELECT * FROM {self.model_cls._table_name} ' \
                f'WHERE {" AND ".join(values_str)}'
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, tuple(values_list))
        except aiomysql.Error as err:
            await cursor.close()
            raise ValueError(str(err.args[1]))
        field_names = [column[0] for column in cursor.description]
        for tuple_arg in await cursor.fetchall():
            kwarg = {}
            for idx, field_name in enumerate(field_names):
                kwarg[field_name] = tuple_arg[idx]
            await cursor.close()
            return self.model_cls(**kwarg)
        await cursor.close()
        raise DoesNotExist(f'{self.model_cls.__name__} {kwargs}')

    async def filter(self, **kwargs):
        values_str = []
        values_list = []
        for column, value in kwargs.items():
            values_str.append(f'{column} = %s')
            values_list.append(value)
        query = f'SELECT * FROM {self.model_cls._table_name} ' \
                f'WHERE {" AND ".join(values_str)}'
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, tuple(values_list))
        except aiomysql.Error as err:
            await cursor.close()
            raise ValueError(str(err.args[1]))
        users_list = []
        field_names = [column[0] for column in cursor.description]
        for tuple_arg in await cursor.fetchall():
            kwarg = {}
            for idx, field_name in enumerate(field_names):
                kwarg[field_name] = tuple_arg[idx]
            users_list.append(self.model_cls(**kwarg))
        await cursor.close()
        return users_list

    async def delete(self, **kwargs):
        values_str = []
        values_list = []
        for column, value in kwargs.items():
            values_str.append(f'{column} = %s')
            values_list.append(value)
        query = f'DELETE FROM {self.model_cls._table_name} ' \
                f'WHERE {" AND ".join(values_str)}'
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, tuple(values_list))
        except aiomysql.Error as err:
            await cursor.close()
            raise ValueError(str(err.args[1]))
        await cursor.close()
        await self.conn.commit()


class Model(metaclass=ModelMeta):
    class Meta:
        table_name = ''

    objects = Manage()

    def __init__(self, **kwargs):
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)

    async def save(self, *update):
        if update:
            await self.objects.update(*update, **self.__dict__)
        elif 'id' in self.__dict__ and self.id is None:
            new = await self.objects.create(**self.__dict__)
            self.id = new.id
        else:
            await self.objects.create(**self.__dict__)

    async def delete(self):
        await self.objects.delete(self.__dict__)


class User(Model):
    id = IntField(pri_key=True, auto_inc=True)
    email = StringField(size=32)
    password = StringField(size=32)
    name = StringField(size=32)
    created_date = DatetimeField()
    last_login_date = DatetimeField()

    class Meta:
        table_name = 'Users'


class Token(Model):
    token = StringField(size=36)
    user_id = IntField()
    expire_date = DatetimeField()

    class Meta:
        table_name = 'Token'

class Stat(Model):
    domain = StringField(size=255)
    status = StringField(size=64)
    author_id = IntField()
    https = IntField(bool=True, required=False, default=0)
    time = DatetimeField()
    pages_count = IntField(required=False, default=0)

    class Meta:
        table_name = 'CrawlerStats'
