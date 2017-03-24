# keep a log of jobs
# - always: experiment id, timestamp
# - when something is launched
#   - job id (platform dependent, e.g. slurm or pid)
#   - which file(s) are being made
# - when it starts running
#   - git commit:  git --git-dir=/path/to/.git rev-parse HEAD  (or: git describe --always)
# - when it finishes running
#   - ended successfully
# - when you check status
#   - parse log to find jobs that should be waiting/running
#       - check their status (platform dependent), log the failed ones
#   - display some monitoring (app and step dependent, e.g. last saved model, dev loss, number of output lines, eval, ...)
# - manually: mark an experiment as ended (won't show up in status list anymore)

import argparse

from .configuration import Config

def get_parser(recipe):
    parser = argparse.ArgumentParser(
        description='TextPipes (recipe: {})'.format(recipe.name))

    parser.add_argument('conf', type=str, metavar='CONF',
                        help='Name of the experiment conf file')
    parser.add_argument('output', type=str, nargs='*', metavar='OUTPUT',
                        help='Output(s) to schedule, in section:key format')
    parser.add_argument('--status', default=False, action='store_true',
                        help='Status of ongoing experiments')
    parser.add_argument('--check', default=False, action='store_true',
                        help='Perform validity check')
    parser.add_argument('--make', default=None, type=str, metavar='OUTPUT',
                        help='Output to make, in section:key format. '
                        'You should NOT call this directly')

    return parser

def main(recipe):
    parser = get_parser(recipe)
    args = parser.parse_args()
    conf = Config(args.conf)

    # debug
    nextsteps = recipe.get_all_next_steps_for(conf)
    print(nextsteps)

    #f len(nextsteps) == 0:
    #    print('all done')
    #else:
    #    for ns in nextsteps[0]:
