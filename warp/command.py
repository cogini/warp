from __future__ import print_function
import sys
from inspect import getargspec

from twisted.python import usage, reflect, log
from twisted.python.filepath import FilePath
from twisted.internet.defer import waitForDeferred, succeed, fail

from warp.webserver import resource, site
from warp.common import store, translate
from warp import runtime

from storm.database import create_database
from storm.twisted.store import StorePool
from storm.uri import URI

from txpostgres import txpostgres

class Options(usage.Options):
    optParameters = (
        ("siteDir", "d", ".", "Base directory of the warp site"),
        ("config", "w", "warpconfig", "Config filename"),
    )

    optFlags = (
        ("skipSchemaCheck", "x", "Don't check schema integrity"),
    )

    subCommands = []

_commands = {}

# NTA XXX: This is not usable by app, because when command-line
# options are parsed app-specific information is not available yet.
# A hacky workaround would be some code in twisted.warp_plugin to
# import a "magic" app-defined module
def register(shortName=None, skipConfig=False, needStartup=False, optionsParser=None):
    """Decorator to register functions as commands. Functions must
    accept options map as the first parameter.

    Usage:

    @register(*params)
    def foo(options, arg1):
        pass
    """
    def decorator(fn):
        name = fn.__name__
        doc = fn.__doc__ or ""

        if optionsParser is None:
            class CmdOptions(usage.Options):
                def parseArgs(self, *args):
                    spec = getargspec(fn)
                    if spec.defaults:
                        raise usage.UsageError("Custom command cannot have arguments with default values")
                    if spec.varargs:
                        raise usage.UsageError("Custom command cannot take variable number of arguments")
                    if spec.keywords:
                        raise usage.UsageError("Custom command cannot take keyword arguments")

                    cmd_args = spec.args[1:]
                    count = len(cmd_args)
                    if len(args) != count:
                        raise usage.UsageError(
                            "Wrong number of arguments, %d expected:\n    twistd warp %s %s"
                            % (count, name, " ".join(["<%s>" % arg for arg in cmd_args])))
                    self["args"] = args
            klass = CmdOptions
        else:
            klass = optionsParser

        Options.subCommands.append((name, shortName, klass, doc))

        def wrapped(options):
            if not skipConfig:
                initialize(options)
            if needStartup:
                doStartup(options)
            fn(options, *options.subOptions.get("args", ()))

        _commands[name] = wrapped
        return wrapped
    return decorator

def maybeRun(options):
    subCommand = options.subCommand

    if subCommand:
        command = _commands[subCommand]
        command(options)
        raise SystemExit

def getSiteDir(options):
    "Get `siteDir` out of `options`"
    return FilePath(options['siteDir'])

def doStartup(options):
    """Execute startup function after checking schema if necessary"""
    from warp.common.schema import getConfig
    if getConfig()["check"]:
        from warp.common import schema
        schema.migrate()

    configModule = reflect.namedModule(options['config'])
    if hasattr(configModule, 'startup'):
        configModule.startup()

def cbPoolStarted(result):
    log.msg("tx_pool started")
    runtime.tx_pool = result

def initialize(options):
    """Load Warp config and intialize"""
    site_dir = FilePath(options['siteDir'])
    sys.path.insert(0, site_dir.path)

    print("Loading config from {}".format(options['config']))
    config_module = reflect.namedModule(options['config'])
    config = config_module.config
    runtime.config.update(config)

    runtime.config['siteDir'] = site_dir
    runtime.config['warpDir'] = FilePath(runtime.__file__).parent()

    if options["skipSchemaCheck"]:
        runtime.config["schema"] = runtime.config.get("schema", {})
        runtime.config["schema"]["check"] = False

    # Set up database
    uri = URI(config['db'])
    database = create_database(uri)

    # Old store with single db connection
    # store.setupStore()
    runtime.avatar_store.__init__(database)

    if config.get('trace'):
        import storm.tracer
        storm.tracer.debug(True, stream=sys.stdout)

    # Store pool
    min_size = config.get('db_pool_min', 5)
    max_size = config.get('db_pool_max', 5)
    pool = StorePool(database, min_size, max_size)
    pool.start()
    runtime.pool = pool
    log.msg("storm pool started")

    tx_pool = txpostgres.ConnectionPool(None, min=1,
                                        dbname=uri.database,
                                        user=uri.username,
                                        password=uri.password,
                                        host=uri.host)
    d = tx_pool.start()
    d.addCallback(cbPoolStarted)

    translate.loadMessages()

    runtime.config['warpSite'] = site.WarpSite(resource.WarpResourceWrapper())

    return config_module

# Pre-defined commands -----------------------------------------------

class SkeletonOptions(usage.Options):
    optParameters = (
        ("siteDir", "d", ".", "Base directory of the warp site to generate"),
    )

@register(skipConfig = True, optionsParser = SkeletonOptions)
def skeleton(options):
    "Copy Warp site skeleton into current directory"
    from warp.tools import skeleton
    print('Creating skeleton...')
    site_dir = getSiteDir(options)
    skeleton.createSkeleton(site_dir)

@register()
def node(options, name):
    "Create a new node"
    from warp.tools import skeleton
    nodes = getSiteDir(options).child('nodes')
    if not nodes.exists():
        print('Please run this from a Warp site directory')
        return
    skeleton.createNode(nodes, name)

@register()
def crud(options, name, model):
    "Create a new CRUD node"
    from warp.tools import autocrud
    nodes = getSiteDir(options).child('nodes')
    if not nodes.exists():
        print('Please run this from a Warp site directory')
        return
    autocrud.autocrud(nodes, name, model)

@register()
def adduser(options):
    "Add a user (interactive)"
    from warp.tools import adduser
    adduser.addUser()

@register(needStartup = True)
def console(options):
    "Python console with Warp runtime available"
    import code
    locals = {'store': runtime.store}
    c = code.InteractiveConsole(locals)
    c.interact()

@register(needStartup = True, shortName = "c")
def command(options, function):
    "Run a site-specific command"
    obj = reflect.namedObject(function)
    obj()

class SchemaOptions(usage.Options):
    optFlags = (
        ("dryRun", "n", "Do a dry-run instead of changing the DB for real"),
    )

@register(optionsParser = SchemaOptions)
def snapshotSchema(options):
    "Snapshot the DB schema (useful for converting existing projects)"
    from warp.common import schema
    schema.snapshot(dryRun = True if options.subOptions["dryRun"] else False)

@register(optionsParser = SchemaOptions)
def migrate(options):
    "Migrate the DB to meet the code's expectation"
    from warp.common import schema
    schema.migrate(dryRun = True if options.subOptions["dryRun"] else False)
