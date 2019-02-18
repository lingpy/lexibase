from urllib.request import urlretrieve

from tqdm import tqdm

__all__ = ['stringval', 'download']


def stringval(val):
    if isinstance(val, (tuple, list)):
        return ' '.join([str(v) for v in val])
    return str(val)


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download(url, output_path):
    with DownloadProgressBar(
            unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]) as t:
        urlretrieve(url, filename=str(output_path), reporthook=t.update_to)
