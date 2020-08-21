import nox


@nox.session(venv_backend='conda')
def tests(session):
    session.conda_install('pandas')
