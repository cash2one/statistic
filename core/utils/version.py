import os

def get_git_revision(path=None):
    """
    Returns the GIT revision in the gitlib,
    where XXXX is the revision number.

    Returns SVN-unknown if anything goes wrong, such as an unexpected
    format of internal SVN files.

    If path is provided, it should be a directory whose SVN info you want to
    inspect. If it's not provided, this will use the root django/ package
    directory.
    """
    rev = None
    try:
        f = os.popen("git log")
        l = f.readline()
        l = l.split()
        if len(l) == 2 and l[0] == "commit":
            rev = l[1]
    except:
        pass
        
    if rev:
        return u'GIT-%s' % rev
    return u'GIT-unknown'
