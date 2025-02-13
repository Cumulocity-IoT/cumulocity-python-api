# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

import os
from tempfile import NamedTemporaryFile

import pytest

from c8y_api.app import CumulocityApi
from c8y_api.model import Binary

from util.testing_util import RandomNameGenerator


@pytest.fixture(scope='session')
def file_factory(logger):
    """Provide a file factory which creates test files and deletes them
    after the session."""
    # pylint: disable=consider-using-with
    created_files = []

    def create_file() -> (str, str):
        data = RandomNameGenerator.random_name(99, ' ')
        file = NamedTemporaryFile(delete=False)
        file.write(bytes(data, 'utf-8'))
        file.close()
        logger.info(f"Created temporary file: {file.name}")
        created_files.append(file.name)
        return file.name, data

    yield create_file

    for f in created_files:
        os.remove(f)
        logger.info(f"Removed temporary file: {f}")


def test_CRUD(live_c8y: CumulocityApi, file_factory):
    """Verify that object based create, update, and delete works as
    expected."""

    file1_name, file1_data = file_factory()
    file2_name, file2_data = file_factory()
    binary = Binary(c8y=live_c8y, name='some_file.py', type='text/raw', file=file1_name, custom_attribute=False)

    # 1) create the managed object and store the file
    binary = binary.create()
    try:

        # -> the returned managed object has all the data
        assert binary.id
        assert binary.is_binary
        assert binary.c8y_IsBinary is not None
        assert binary.custom_attribute is False
        assert binary.content_type == binary.type

        # -> the file data matches what we have on disk
        assert file1_data == binary.read_file().decode('utf-8')

        # 2) update the stored file
        binary.file = file2_name
        binary = binary.update()

        # -> the file data matches what we have on disk
        assert file2_data == binary.read_file().decode('utf-8')

        # 3) delete the binary
        binary.delete()

        # -> cannot be found anymore
        with pytest.raises(KeyError):
            live_c8y.binaries.read_file(binary.id)

    except Exception as e:
        binary.delete()
        raise e


def test_CRUD2(live_c8y: CumulocityApi, file_factory):
    """Verify that API based create, update, and delete works as expected."""

    file1_name, file1_data = file_factory()
    file2_name, file2_data = file_factory()

    # 1) upload a binary file
    created = live_c8y.binaries.upload(file=file1_name, name='test.txt', type='text/raw')

    # -> the returned managed object has all the metadata
    assert created.id
    assert created.is_binary
    assert created.c8y_IsBinary is not None
    assert created.content_type == created.type

    # 2) read the file contents
    content = live_c8y.binaries.read_file(created.id)

    # -> matches what we have
    assert content.decode('utf-8') == file1_data

    # 3) update the file
    live_c8y.binaries.update(created.id, file=file2_name)

    # -> matches what we have
    content = live_c8y.binaries.read_file(created.id)
    assert content.decode('utf-8') == file2_data

    # 4) delete the file
    live_c8y.binaries.delete(created.id)
