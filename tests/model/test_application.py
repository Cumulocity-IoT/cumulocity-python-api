# Copyright (c) 2025 Cumulocity GmbH

import json
import os

from c8y_api.model import Application


def test_parsing():
    """Verify that parsing an Application from JSON works."""
    path = os.path.dirname(__file__) + '/application.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        application_json = json.load(f)
    application = Application.from_json(application_json)

    assert application.id == application_json['id']
    assert application.type == application_json['type']
    assert application.availability == application_json['availability']
    assert application.owner == application_json['owner']['tenant']['id']
