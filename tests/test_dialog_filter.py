"""Group/channel list keyword filtering is local; never searches messages."""
from types import SimpleNamespace

from tgdl.dialog_filter import filter_dialogs


def test_filter_group_channel_names_chinese_and_casefold():
    rows = [
        ("-10011", "supergroup", "摄影交流群", "photography"),
        ("-10012", "channel", "Python 技术频道", "py_updates"),
        ("-10013", "group", "读书会", ""),
    ]
    assert filter_dialogs(rows, "摄影") == rows[:1]
    assert filter_dialogs(rows, "python") == rows[1:2]
    assert filter_dialogs(rows, "UPDATES") == rows[1:2]
    assert filter_dialogs(rows, "  ") == rows
    assert filter_dialogs(rows, "不存在") == []


def test_filter_dialog_info_by_name_or_username_without_network():
    rows = [
        SimpleNamespace(entity_id=1, title="Linux 交流群", username="linux_world"),
        SimpleNamespace(entity_id=2, title="电影分享频道", username=None),
    ]
    assert filter_dialogs(rows, "电影") == [rows[1]]
    assert filter_dialogs(rows, "LINUX_WORLD") == [rows[0]]
    assert filter_dialogs(rows, "") == rows


def test_filter_does_not_match_message_contents_or_identifiers():
    # The only searchable fields are group/channel name and username.
    rows = [("-10099", "group", "技术交流", "mygroup")]
    assert filter_dialogs(rows, "secret inside a message") == []
    assert filter_dialogs(rows, "-10099") == []
