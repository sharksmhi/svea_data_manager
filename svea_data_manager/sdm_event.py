_sdm_subscribers = dict(
    on_resource_added={},
    on_resource_rejected={},
    on_stop_write={},
    on_target_path_exists={},
    on_file_copied={},
    log={},
    on_transform_add_file={}
)


class SDMEventNotFound(Exception):
    pass


def get_events():
    return sorted(_sdm_subscribers)


def subscribe(event: str, func, prio=50):
    if event not in _sdm_subscribers:
        raise SDMEventNotFound(event)
    _sdm_subscribers[event].setdefault(prio, [])
    _sdm_subscribers[event][prio].append(func)


def post_event(event: str, data):
    if event not in _sdm_subscribers:
        raise SDMEventNotFound(event)
    for prio in sorted(_sdm_subscribers[event]):
        for func in _sdm_subscribers[event][prio]:
            func(data)

