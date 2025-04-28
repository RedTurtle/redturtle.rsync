# -*- coding: utf-8 -*-
# documentazione: ....
from zope.interface import Interface
from Acquisition import aq_base
import logging
from plone import api
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from plone.namedfile.file import NamedBlobImage
from z3c.relationfield.relation import RelationValue
from zope.lifecycleevent import ObjectModifiedEvent
from zope.event import notify
from Products.Five.utilities.marker import mark

logger = logging.getLogger(__name__)


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super(TimeoutHTTPAdapter, self).__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super(TimeoutHTTPAdapter, self).send(request, **kwargs)


# https://dev.to/ssbozy/python-requests-with-retries-4p03
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 501, 502, 503, 504),
    timeout=5.0,
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    # adapter = HTTPAdapter(max_retries=retry)
    adapter = TimeoutHTTPAdapter(max_retries=retry, timeout=timeout)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

http = requests_retry_session(retries=7, timeout=30.0)


class ISynced(Interface):
    """marker for synced content"""


def json_extractor(container, response, **kwargs):
    return response.json()


def image_extractor(container, response, **kwargs):
    if not response.headers.get('content-type', '').startswith('image/'):
        logger.error('invalid for image_extractor %s (%r)', response.url, response.headers.get('content-type', ''))
        return None
    return {
        'title': kwargs.get('name', response.url.split('/')[-1]),
        'image': NamedBlobImage(data=response.content, filename=response.url.split('/')[-1])
    }


def page_creator(container, data, id=None, portal_type='Document', **kwargs):
    obj = api.content.create(container, type=portal_type, id=id, **data)
    logger.warning('created %s', obj.absolute_url())
    if 'review_state' in kwargs:
        if api.content.get_state(obj) != kwargs['review_state']:
            api.content.transition(obj, to_state=kwargs['review_state'])
        # try:
        #     api.content.transition(obj, to_state=kwargs['review_state'])
        # except api.exc.InvalidParameterError:
        #     logger.error('unable to set transition state for %s to %s', obj.absolute_url(), kwargs['review_state'])
    mark(obj, ISynced)
    obj.reindexObject(idxs=['object_provides'])
    return obj


def page_delete(obj):
    logger.warning('delete %s', obj.absolute_url())
    api.content.delete(obj)
    return None


def page_update(obj, data, **kwargs):
    changed_fields = []
    for fieldname, new_value in data.items():
        # TODO: verificare che il fieldname sia nello schema dell'obj ?
        # TODO: qual'è il modo corretto/generale di fare setter di un field ?
        # TODO: vedere z3c.form e come fa lui a vedere se le modifiche sono effettive
        #       o se non è stato modificato nulla ?
        old_value = getattr(aq_base(obj), fieldname, None)
        if isinstance(new_value, RelationValue) and isinstance(old_value, RelationValue):
            changed = (new_value.to_id != old_value.to_id)
        else:
            changed = (new_value != old_value)
        if changed:
            setattr(obj, fieldname, new_value)
            changed_fields.append(fieldname)
    if changed_fields:
        notify(ObjectModifiedEvent(obj))
        # BBB: la reindexObject modifica la modification_date, la azzeriamo
        # di nuovo col valore originale se esiste
        if data.get('modification_date'):
            setattr(obj, 'modification_date', data['modification_date'])
            obj.reindexObject(idxs=['modified'])
    logger.warning('update %s fields:%r', obj.absolute_url(), changed_fields)
    return obj


def obj_getter(container, remoteid):
    return container.get(remoteid)


# BBB: usare parametri o adapter ?
def rsync(container,
          remoteid,
          remoteurl=None,
          data=None,
          force_update=False,
          extractor=json_extractor,
          getter=obj_getter,
          creator=page_creator,
          updater=page_update,
          deleter=page_delete,
          verbose=False,
          **kwargs):
    """
    * container: destination plone container
    * remoteid: pageid (destinaton pageid, i.e. remote uuid)
    * remoteurl:

    # TODO: usare if-modified-since dove possibile
    # TODO: valutare eventualmente una funzione per definire l'id del contenuto locale
    """
    if not remoteurl and not data:
        raise Exception('remoteurl or data required')
    obj = getter(container, remoteid)
    if remoteurl:
        response = http.get(remoteurl)
    else:
        response = data
    if obj:
        # update or delete
        if not response:
            # delete (se da 5XX non si cancella...)
            if response.status_code in ['401', '403', '404']:
                return deleter(obj)
            else:
                # TODO: sollevare un'eccezione quando c'è un errore in modo
                # che l'update venga fatto al sync sucessivo?
                logger.error('unable to fetch %s (%s)', remoteurl, response.status_code)
                return None
        else:
            # TODO: verifica sulle date di aggiornamento della pagina remota vs. locale
            data = extractor(container, response, **kwargs)
            if verbose:
                # TODO
                logger.warning('DEBUG: %s', data)
            if data:
                # default: se non ci sono dati di ultima modifica non si fanno
                #          modifiche
                update = False
                if 'modification_date' in data:
                    # BBB: le due date devono esistere ed essere entrambe DateTime
                    update = (data['modification_date'] > obj.modification_date)
                if update or force_update:
                    return updater(obj, data, **kwargs)
            else:
                # se la pagina remota non ha i metadati e come se fosse stata cancellate
                # quindi va cancllata anche quella locale
                return deleter(obj)
            return obj
    else:
        # create
        if not response:
            logger.error('unable to fetch %s (%s)', remoteurl, response.status_code)
            return None
        else:
            data = extractor(container, response, **kwargs)
            if data:
                obj = creator(container, data, id=remoteid, **kwargs)
                return obj


"""
# ESEMPIO: ALMA2021 vs. Magazine
from unibo.api.rsync import rsync
remoteurl = 'http://magazine.dev.dsaw.unibo.it/archivio/2018/mio-articolo-con-il-nuovo-font'
remoteid = '6fc2a87d4aa64cc7ad6b5bd0838a4c0c'  # AKA http://magazine.dev.dsaw.unibo.it/archivio/2018/mio-articolo-con-il-nuovo-font/uuid

def magazine_extractor(response, lang):
    data = extruct.extract(response.text)
    return data

container = api.content.get('/alma2021/it/notizie')
obj_it = rsync(container, remoteid, remoteurl, extractor=magazine_extractor, lang='it')
container = api.content.get('/alma2021/en/news')
obj_en = rsync(container, remoteid, remoteurl, extractor=magazine_extractor, lang='en')
"""
