#include <Python.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
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
    const char* output_dir;
    int proc_num;
    PyObject *input1, *input2, *input3, *input4;
    if (!PyArg_ParseTuple(args, "siOOOO", &output_dir, &proc_num, &input1, &input2,
                          &input3, &input4)) {
        return NULL;
    }

    printf("_fasttrips_initialize_supply for output_dir %s proc num %d\n", output_dir, proc_num);
    // access_links index: TAZ id, stop id
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input1, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* acc_indexes= (int*)PyArray_DATA(pyo);
    int num_indexes = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // access_links cost: time, access cost, egress cost
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input2, NPY_FLOAT32, 2, 2);
    float* costs    = (float*)PyArray_DATA(pyo);
    int num_costs   = PyArray_DIMS(pyo)[0];
    assert(3 == PyArray_DIMS(pyo)[1]);

    // these better be the same length
    assert(num_indexes == num_costs);

    // trip stop times index: trip id, sequence, stop id
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input3, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* stop_indexes   = (int*)PyArray_DATA(pyo);
    int num_stop_ind    = PyArray_DIMS(pyo)[0];
    assert(3 == PyArray_DIMS(pyo)[1]);

    // trip stop times data: arrival time, departure time
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input4, NPY_FLOAT32, 2, 2);
    float* stop_times   = (float*)PyArray_DATA(pyo);
    int num_stop_times  = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // these better be the same length
    assert(num_stop_ind == num_stop_times);

    // keep them
    pathfinder.initializeSupply(output_dir, proc_num,
                                acc_indexes, costs, num_indexes,
                                stop_indexes, stop_times, num_stop_ind);

    Py_RETURN_NONE;
}

static PyObject *
_fasttrips_find_path(PyObject *self, PyObject *args)
{
    PyArrayObject *pyo;
    fasttrips::PathSpecification path_spec;
    int   hyperpath_i, outbound_i, trace_i;
    if (!PyArg_ParseTuple(args, "iiiiifi", &path_spec.path_id_, &hyperpath_i,
                          &path_spec.origin_taz_id_, &path_spec.destination_taz_id_,
                          &outbound_i, &path_spec.preferred_time_, &trace_i)) {
        return NULL;
    }
    path_spec.hyperpath_  = (hyperpath_i != 0);
    path_spec.outbound_   = (outbound_i  != 0);
    path_spec.trace_      = (trace_i     != 0);
    pathfinder.findPath(path_spec);

    Py_RETURN_NONE;
}

static PyMethodDef fasttripsMethods[] = {
    {"system",              _fasttrips_system,            METH_VARARGS, "Execute a shell command."  },
    {"initialize_supply",   _fasttrips_initialize_supply, METH_VARARGS, "Initialize network supply" },
    {"find_path",           _fasttrips_find_path,         METH_VARARGS, "Find trip-based path"      },
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
