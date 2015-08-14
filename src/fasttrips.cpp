#include <Python.h>

#include <numpy/arrayobject.h>

#include "pathfinder.h"
#include <string>
#include <queue>

static PyObject *pyError;

// global variable?
fasttrips::PathFinder pathfinder;

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

static PyObject *
_fasttrips_initialize_supply(PyObject *self, PyObject *args)
{
    PyArrayObject *pyo;
    int proc_num;
    PyObject *input1, *input2, *input3;
    if (!PyArg_ParseTuple(args, "iOO", &proc_num, &input1, &input2)) {
        return NULL;
    }

    printf("_fasttrips_initialize_supply for proc num %d\n", proc_num);
    // access_links index: TAZ id + stop id
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input1, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* indexes    = (int*)PyArray_DATA(pyo);
    int num_indexes = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // access_links cost
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input2, NPY_FLOAT32, 1, 1);
    float* costs    = (float*)PyArray_DATA(pyo);
    int num_costs   = PyArray_DIMS(pyo)[0];

    // these better be the same length
    assert(num_indexes == num_costs);

    // keep them
    pathfinder.initializeSupply(proc_num, indexes, costs, num_indexes);

    Py_RETURN_NONE;
}

static PyMethodDef fasttripsMethods[] = {
    {"system",  _fasttrips_system, METH_VARARGS, "Execute a shell command."},
    {"initialize_supply", _fasttrips_initialize_supply, METH_VARARGS, "initialize_supply"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
init_fasttrips(void)
{
    printf("init_fasttrips called\n");

    std::string x = "all animals want to live";
    printf("string x = [%s]\n", x.c_str());

    std::priority_queue<std::string> myqueue;

    PyObject *m = Py_InitModule("_fasttrips", fasttripsMethods);
    if (m == NULL)
        return;

    import_array();

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