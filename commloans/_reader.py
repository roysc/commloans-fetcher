import os


class Reader:
  def __init__(self, root):
    self.root= os.path.realpath(root)
    assert os.path.exists(self.root), self.root

  def __repr__(self):
    return '%s(%s)' % (type(self).__name__, repr(self.root))
