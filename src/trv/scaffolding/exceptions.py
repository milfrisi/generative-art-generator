class ScaffoldingError(Exception):
    pass

class ConfError(ScaffoldingError):
    pass

class OozieError(ScaffoldingError):
    pass

class HiveError(ScaffoldingError):
    pass

class KerberosError(ScaffoldingError):
    pass

