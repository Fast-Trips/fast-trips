#include <Python.h>
#include <string>
#include <queue>

static PyObject *pyError;

static PyObject *
_fasttrips_system(PyObject *self, PyObject *args)
{
    const char *command;
    int sts;

    if (!PyArg_ParseTuple(args, "s", &command))
        return NULL;
    sts = system(command);
    return Py_BuildValue("i", sts);
}

static PyMethodDef fasttripsMethods[] = {
    {"system",  _fasttrips_system, METH_VARARGS, "Execute a shell command."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
init_fasttrips(void)
{
    PyObject *m;

    printf("init_fasttrips called\n");

    std::string x = "all animals want to live";
    printf("string x = [%s]\n", x.c_str());

    std::priority_queue<std::string> myqueue;

    m = Py_InitModule("_fasttrips", fasttripsMethods);
    if (m == NULL)
        return;

    pyError = PyErr_NewException("_fasttrips.error", NULL, NULL);
    Py_INCREF(pyError);
    PyModule_AddObject(m, "error", pyError);
}

int
main(int argc, char *argv[])
{
    /* Pass argv[0] to the Python interpreter */
    Py_SetProgramName(argv[0]);

    /* Initialize the Python interpreter.  Required. */
    Py_Initialize();

    /* Add a static module */
    init_fasttrips();
}