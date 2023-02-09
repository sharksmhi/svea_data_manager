_sdm_subscribers = dict(
    on_resource_added={},
    on_resource_rejected={},
    on_stop_write={},
    on_target_path_exists={},
    on_file_storage_copied={},
    on_svn_storage_prepared={},
    on_svn_storage_progress={},
    log={},
    on_transform_add_file={},
    before_read_packages={},
    after_read_packages={},
    before_transform_packages={},
    after_transform_packages={},
    before_write_packages={},
    after_write_packages={},
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


def post_event(event: str, data=None):
    if event not in _sdm_subscribers:
        raise SDMEventNotFound(event)
    for prio in sorted(_sdm_subscribers[event]):
        for func in _sdm_subscribers[event][prio]:
            func(data)

