from datetime import datetime
from urllib.parse import urlencode
from c8y_rest import get, post, delete


class _DictWrapper:

    def __init__(self, dictionary):
        self.items = dictionary

    def has(self, name):
        return name in self.items

    def __getattr__(self, name):
        item = self.items[name]
        return item if not isinstance(item, dict) else _DictWrapper(item)


class Fragment:

    def __init__(self, name, **kwargs):
        self.name = name
        self.items = kwargs

    @staticmethod
    def _from_json(name, body_json):
        f = Fragment(name)
        f.items = body_json
        return f

    def __getattr__(self, name):
        item = self.items[name]
        return item if not isinstance(item, dict) else _DictWrapper(item)

    def has(self, element_name):
        return element_name in self.items

    def add_element(self, name, element):
        self.items[name] = element
        return self


class ManagedObject:

    def __init__(self, type, name, owner, *fragments):
        self.id = None
        self.__creation_time = None
        self.__creation_datetime = None
        self.__update_time = None
        self.__update_datetime = None
        self.type = type
        self.name = name
        self.owner = owner if owner else ''  # todo: get current user from c8y
        self.child_device_ids = []
        self.child_assets = []
        self.child_additions = []
        self.parent_devices = []
        self.parent_assets = []
        self.parent_additions = []
        fs = fragments[0] if isinstance(fragments[0], list) else fragments
        self.fragments = {f.name: f for f in fs}
        self.is_device = False

    @property
    def creation_time(self):
        if not self.__creation_time:
            if not self.__creation_datetime:
                return None
            self.__creation_time = self.__creation_datetime.isoformat(timespec='milliseconds')
        return self.__creation_datetime

    @property
    def creation_datetime(self):
        if not self.__creation_datetime:
            if not self.__creation_time:
                return None
            self.__creation_datetime = datetime.fromisoformat(self.__creation_time)
        return self.__creation_datetime

    @property
    def update_time(self):
        if not self.__update_time:
            if not self.__update_datetime:
                return None
            self.__update_time = self.__update_datetime.isoformat(timespec='milliseconds')
        return self.__update_datetime

    @property
    def update_datetime(self):
        if not self.__update_datetime:
            if not self.__update_time:
                return None
            self.__update_datetime = datetime.fromisoformat(self.__update_time)
        return self.__update_datetime

    def store(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError


class Device(ManagedObject):

    def __init__(self, type, name, owner, *fragments):
        ManagedObject.__init__(type, name, owner, *fragments)
        self.is_device = True


class Measurement:

    __BUILTIN_FRAGMENTS = ['type', 'id', 'source', 'time', 'self']

    def __init__(self, type, source, time=None, fragments=None):
        self.id = ''
        self.type = type
        self.source = source
        self.__datetime = time if isinstance(time, datetime) else None
        self.__time = time if isinstance(time, str) else None
        # fragments can be either just a simple dictionary (i.e. json) or
        # a collection (incl. single) of Fragment instances which need
        # to be unpacked before using them
        self.fragments = Measurement.__ensure_dict(fragments) if fragments else {}

    @staticmethod
    def _from_json(measurement_json):
        id = measurement_json['id']
        type = measurement_json['type']
        source = measurement_json['source']['id']
        time = measurement_json['time']
        fragments = {name: body for name, body in measurement_json.items()
                     if name not in Measurement.__BUILTIN_FRAGMENTS}
        m = Measurement(type, source, time, fragments)
        m.id = id
        return m

    @staticmethod
    def __ensure_dict(fragments):
        if isinstance(fragments, dict):
            return fragments
        if isinstance(fragments, Fragment):
            return {fragments.name: fragments.items}
        if isinstance(fragments, list):
            return {f.name: f.items for f in fragments}
        raise TypeError("Unexpected argument type: " + str(type(fragments)))

    def __getattr__(self, item):
        return _DictWrapper(self.fragments[item])

    def has(self, fragment_name):
        return fragment_name in self.fragments

    def add_fragment(self, name, **kwargs):
        self.fragments[name] = kwargs
        return self

    def add_fragments(self, *fragments):
        if len(fragments) == 1 and isinstance(fragments, list):
            return self.add_fragments(*fragments[0])
        for fragment in fragments:
            self.fragments[fragment.name] = fragment.items
        return self

    @property
    def datetime(self):
        if not self.__datetime:
            if not self.__time:
                self.now()
            self.__datetime = datetime.fromisoformat(self.__time)
        return self.__datetime

    @property
    def time(self):
        if not self.__time:
            if not self.__datetime:
                self.now()
            self.__time = self.__datetime.isoformat(timespec='milliseconds')
        return self.__time

    def now(self):
        self.__datetime = datetime.now()
        self.__time = self.__datetime.isoformat(timespec='milliseconds')

    def store(self):
        if not self.__time:
            self.now()
        body_json = {
            'type': self.type,
            'source': {'id': self.source},
            'time': self.__time}
        body_json.update({name: body for name, body in self.fragments.items()})
        post('/measurement/measurements', body_json)

    def delete(self):
        delete('/measurement/measurements/' + self.id)


class Measurements:

    @staticmethod
    def get(id=""):
        pass

    @staticmethod
    def select(type="", source="", fragment="", before="", after="", reverse=False, page_size=1000):
        """Lazy implementation."""
        base_query = Measurements.__build_base_query(type=type, source=source, fragment=fragment,
                                                     before=before, after=after,
                                                     reverse=reverse, block_size=page_size)
        page_number = 0
        while True:
            results = [Measurement._from_json(x) for x in Measurements.__get_page(base_query, page_number)]
            if not results:
                break
            for result in results:
                yield result
            page_number = page_number + 1

    @staticmethod
    def get_all(type="", source="", fragment="", before="", after="", reverse=False, block_size=1000):
        """Will get everything and return as a single result."""
        return [x for x in Measurements.select(type, source, fragment, before, after, reverse, block_size)]

    @staticmethod
    def get_last(type="", source="", fragment=""):
        """Will just get the last available measurement."""
        query = Measurements.__build_base_query(type=type, source=source, fragment=fragment, reverse=True, block_size=1)
        return Measurement._from_json(get(query + "0")['measurements'][0])

    @staticmethod
    def store(*measurements):
        if len(measurements) == 1 and isinstance(measurements[0], list):
            Measurements.store(*measurements)
        else:
            for m in measurements:
                m.store()

    @staticmethod
    def __build_base_query(type="", source="", fragment="", before="", after="", reverse=False, block_size=1000):
        # todo: before and after could be actual dates, not strings
        # prepare map of parameters (ignore None ones) to append
        params = {k: v for k, v in {'type': type, 'source': source, 'fragmentType': fragment,
                                    'dateFrom': after, 'dateTo': before, 'reverse': str(reverse),
                                    'pageSize': block_size}.items() if v}
        assert params  # there needs to be at least 1 param for the next line to make sense
        return '/measurement/measurements?' + urlencode(params) + '&currentPage='

    @staticmethod
    def __get_page(base_query, page):
        result = get(base_query + str(page))
        return result['measurements']

