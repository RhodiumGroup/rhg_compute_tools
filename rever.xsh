$PROJECT = 'rhg_compute_tools'

from rever.activity import activity

@activity
def run_tests():
    ''' Running pytest '''
    pytest

$ACTIVITIES = [
    'run_tests', # run the test suite before allowing version bumps
    'version_bump',  # Changes the version number in various source files (setup.py, __init__.py, etc)
    'changelog',  # Uses files in the news folder to create a changelog for release
    'tag',  # Creates a tag for the new version number
    'push_tag',  # Pushes the tag up to the $TAG_REMOTE
    'ghrelease'  # Creates a Github release entry for the new tag
    ]

$VERSION_BUMP_PATTERNS = [  # These note where/how to find the version numbers
                         ('rhg_compute_tools/__init__.py', '__version__\s*=.*', "__version__ = '$VERSION'"),
                         ('setup.py', 'version\s*=.*,', "version='$VERSION',")
                         ]
$CHANGELOG_FILENAME = 'HISTORY.rst'  # Filename for the changelog
$PUSH_TAG_REMOTE = 'git@github.com:RhodiumGroup/rhg_compute_tools.git'  # Repo to push tags to

$GITHUB_ORG = 'RhodiumGroup'  # Github org for Github releases and conda-forge
$GITHUB_REPO = 'rhg_compute_tools'  # Github repo for Github releases  and conda-forge
