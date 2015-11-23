#include <Python.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>

#include "pathfinder.h"
#include <string>
#include <queue>

static PyObject *pyError;

// global variable
fasttrips::PathFinder pathfinder;

static PyObject *
_fasttrips_initialize_parameters(PyObject *self, PyObject *args)
{
    double     time_window;
    double     bump_buffer;
    int        stoch_pathset_size;
    double     stoch_dispersion;
    if (!PyArg_ParseTuple(args, "ddid", &time_window, &bump_buffer, &stoch_pathset_size, &stoch_dispersion)) {
        return NULL;
    }
    pathfinder.initializeParameters(time_window, bump_buffer, stoch_pathset_size, stoch_dispersion);
    Py_RETURN_NONE;

}

static PyObject *
_fasttrips_initialize_costcoeffs(PyObject *self, PyObject *args)
{
    double in_vehicle_time_weight;
    double wait_time_weight;
    double walk_access_time_weight;
    double walk_egress_time_weight;
    double walk_transfer_time_weight;
    double transfer_penalty;
    double schedule_delay_weight;
    double fare_per_boarding;
    double value_of_time;
    if (!PyArg_ParseTuple(args, "ddddddddd",
            &in_vehicle_time_weight,
            &wait_time_weight,
            &walk_access_time_weight,
            &walk_egress_time_weight,
            &walk_transfer_time_weight,
            &transfer_penalty,
            &schedule_delay_weight,
            &fare_per_boarding,
            &value_of_time)) {
        return NULL;
    }
    pathfinder.initializeCostCoefficients(in_vehicle_time_weight,
                                          wait_time_weight,
                                          walk_access_time_weight,
                                          walk_egress_time_weight,
                                          walk_transfer_time_weight,
                                          transfer_penalty,
                                          schedule_delay_weight,
                                          fare_per_boarding,
                                          value_of_time);
    Py_RETURN_NONE;

}

static PyObject *
_fasttrips_initialize_supply(PyObject *self, PyObject *args)
{
    PyArrayObject *pyo;
    const char* output_dir;
    int proc_num;
    PyObject *input3, *input4, *input5, *input6, *input7;
    if (!PyArg_ParseTuple(args, "siOOOOO", &output_dir, &proc_num,
                          &input3, &input4, &input5, &input6, &input7)) {
        return NULL;
    }

    // trip stop times index: trip id, sequence, stop id
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input3, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* stop_indexes   = (int*)PyArray_DATA(pyo);
    int num_stop_ind    = PyArray_DIMS(pyo)[0];
    assert(3 == PyArray_DIMS(pyo)[1]);

    // trip stop times data: arrival time, departure time
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input4, NPY_DOUBLE, 2, 2);
    if (pyo == NULL) return NULL;
    double* stop_times  = (double*)PyArray_DATA(pyo);
    int num_stop_times  = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // these better be the same length
    assert(num_stop_ind == num_stop_times);

    // stop transfers index: from stop id, to stop id
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input5, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* xfer_indexes   = (int*)PyArray_DATA(pyo);
    int num_xfer_ind    = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // stop transfers data: time, cost
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input6, NPY_DOUBLE, 2, 2);
    if (pyo == NULL) return NULL;
    double* xfer_data  = (double*)PyArray_DATA(pyo);
    int num_xfer_data  = PyArray_DIMS(pyo)[0];
    assert(2 == PyArray_DIMS(pyo)[1]);

    // these better be the same length
    assert(num_xfer_ind == num_xfer_data);

    // trips data: trip id number, route id number
    pyo                 = (PyArrayObject*)PyArray_ContiguousFromObject(input7, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* trip_data     = (int*)PyArray_DATA(pyo);
    int num_trip_data  = PyArray_DIMS(pyo)[0];
    assert(3 == PyArray_DIMS(pyo)[1]);

    // keep them
    pathfinder.initializeSupply(output_dir, proc_num,
                                stop_indexes, stop_times, num_stop_ind,
                                xfer_indexes, xfer_data, num_xfer_ind,
                                trip_data, num_trip_data);

    if (proc_num <= 1) {
        std::cout << "RAND_MAX = " << RAND_MAX << std::endl;
    }
    Py_RETURN_NONE;
}

static PyObject *
_fasttrips_set_bump_wait(PyObject* self, PyObject *args)
{
    PyObject *input1, *input2;
    if (!PyArg_ParseTuple(args, "OO", &input1, &input2)) {
        return NULL;
    }
    PyArrayObject *pyo;

    // bump wait index: trip id, stop sequence, stop id
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input1, NPY_INT32, 2, 2);
    if (pyo == NULL) return NULL;
    int* bw_index = (int*)PyArray_DATA(pyo);
    int num_bw    = PyArray_DIMS(pyo)[0];
    assert(3 == PyArray_DIMS(pyo)[1]);

    // bump wait data: arrival time
    pyo             = (PyArrayObject*)PyArray_ContiguousFromObject(input2, NPY_DOUBLE, 1, 1);
    if (pyo == NULL) return NULL;
    double* bw_times= (double*)PyArray_DATA(pyo);
    int num_times   = PyArray_DIMS(pyo)[0];
    assert(num_times == num_bw);

    pathfinder.setBumpWait(bw_index, bw_times, num_bw);
    Py_RETURN_NONE;
}

static PyObject *
_fasttrips_find_path(PyObject *self, PyObject *args)
{
    PyArrayObject *pyo;
    fasttrips::PathSpecification path_spec;
    int   hyperpath_i, outbound_i, trace_i;
    char *user_class, *access_mode, *transit_mode, *egress_mode;
    if (!PyArg_ParseTuple(args, "iiiissssiiidi", &path_spec.iteration_, &path_spec.passenger_id_, &path_spec.path_id_, &hyperpath_i,
                          &user_class, &access_mode, &transit_mode, &egress_mode,
                          &path_spec.origin_taz_id_, &path_spec.destination_taz_id_,
                          &outbound_i, &path_spec.preferred_time_, &trace_i)) {
        return NULL;
    }
    path_spec.hyperpath_  = (hyperpath_i != 0);
    path_spec.outbound_   = (outbound_i  != 0);
    path_spec.trace_      = (trace_i     != 0);
    path_spec.user_class_  = user_class;
    path_spec.access_mode_ = access_mode;
    path_spec.transit_mode_= transit_mode;
    path_spec.egress_mode_ = egress_mode;

    fasttrips::Path path;
    fasttrips::PathInfo path_info = {0, 0, false, 0, 0};
    pathfinder.findPath(path_spec, path, path_info);

    // package for returning.  We'll separate ints and doubles.
    npy_intp dims_int[2];
    dims_int[0] = path.stops_.size();
    dims_int[1] = 6; // stop_id, deparr_mode_, trip_id_, stop_succpred_, seq_, seq_succpred_
    PyArrayObject *ret_int = (PyArrayObject *)PyArray_SimpleNew(2, dims_int, NPY_INT32);

    npy_intp dims_double[2];
    dims_double[0] = path.stops_.size();
    dims_double[1] = 5; // label_, deparr_time_, link_time_, cost_, arrdep_time_
    PyArrayObject *ret_double = (PyArrayObject *)PyArray_SimpleNew(2, dims_double, NPY_DOUBLE);

    for (int ind = 0; ind < dims_int[0]; ++ind) {
        int stop_id = path.stops_[ind];
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 0) = stop_id;
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 1) = path.states_[stop_id].deparr_mode_;
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 2) = path.states_[stop_id].trip_id_;
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 3) = path.states_[stop_id].stop_succpred_;
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 4) = path.states_[stop_id].seq_;
        *(npy_int32*)PyArray_GETPTR2(ret_int, ind, 5) = path.states_[stop_id].seq_succpred_;

        *(npy_double*)PyArray_GETPTR2(ret_double, ind, 0) = path.states_[stop_id].label_;
        *(npy_double*)PyArray_GETPTR2(ret_double, ind, 1) = path.states_[stop_id].deparr_time_;
        *(npy_double*)PyArray_GETPTR2(ret_double, ind, 2) = path.states_[stop_id].link_time_;
        *(npy_double*)PyArray_GETPTR2(ret_double, ind, 3) = path.states_[stop_id].cost_;
        *(npy_double*)PyArray_GETPTR2(ret_double, ind, 4) = path.states_[stop_id].arrdep_time_;
    }

    PyObject *returnobj = Py_BuildValue("(OOd)",ret_int,ret_double,path_info.cost_);
    return returnobj;
}

static PyMethodDef fasttripsMethods[] = {
    {"initialize_parameters",   _fasttrips_initialize_parameters, METH_VARARGS, "Initialize path finding parameters" },
    {"initialize_costcoeffs",   _fasttrips_initialize_costcoeffs, METH_VARARGS, "Initialize cost coefficients"       },
    {"initialize_supply",       _fasttrips_initialize_supply,     METH_VARARGS, "Initialize network supply" },
    {"set_bump_wait",           _fasttrips_set_bump_wait,         METH_VARARGS, "Update bump wait"          },
    {"find_path",               _fasttrips_find_path,             METH_VARARGS, "Find trip-based path"      },
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
init_fasttrips(void)
{
    // printf("init_fasttrips called\n");
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
