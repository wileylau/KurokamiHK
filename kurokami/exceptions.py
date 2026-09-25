'''Exceptions raised by the kurokami library.

The CLI layer catches these and turns them into sys.exit(1), keeping the
library itself free of process exit side effects.
'''


class KurokamiError(Exception):
    '''Base class for all kurokami errors.'''


class NoResultsError(KurokamiError):
    '''Raised when a search yields no listing divs at all, or the serialized
    snapshot is missing/unusable.'''


class NoValidItemsError(KurokamiError):
    '''Raised when listing divs exist but none survive parsing or the
    keyword blacklist.'''