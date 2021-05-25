import concurrent.futures
import os
from functools import partial

from funcy import log_durations


def explore_path(fs, path):
    for root, dirs, files in fs.walk(path):
        path_join = partial(os.path.join, root)
        return list(map(path_join, dirs)), list(map(path_join, files))


def list_files(fs, path, jobs=None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as executor:
        futures = {executor.submit(explore_path, fs, path): path}  # bootstrap

        while futures:
            new_dirs = []

            # Wait for the next future to complete.
            done, _not_done = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future in done:
                _ = futures.pop(future)
                _dirs, files = future.result()

                yield from files
                new_dirs.extend(_dirs)

            futures.update(
                {executor.submit(explore_path, fs, d): d for d in new_dirs}
            )


def list_files_non_threaded(fs, path):
    for root, _, files in fs.walk(path):
        for file in files:
            yield os.path.join(root, file)


if __name__ == "__main__":
    from dvc.fs.local import LocalFileSystem

    with log_durations(print):
        list(list_files(LocalFileSystem(), "."))

    with log_durations(print):
        list(list_files_non_threaded(LocalFileSystem(), "."))
