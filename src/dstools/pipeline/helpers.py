from pathlib import Path

from jinja2 import Template

from dstools.pipeline.tasks import PythonCallable, Null
from dstools.pipeline.products import File


def _null():
    pass


def partitioned_execution(upstream_partitioned,
                          downstream_callable,
                          downstream_prefix,
                          downstream_path,
                          partition_ids,
                          partition_template='partition={{id}}',
                          upstream_other=None):
    # make sure output is a File
    assert isinstance(upstream_partitioned.product, File)

    dag = upstream_partitioned.dag
    partition_template_w_suffix = Template(
        downstream_prefix + '_' + partition_template)
    partition_template_t = Template(partition_template)

    # instantiate null tasks
    nulls = [Null(product=File(Path(str(upstream_partitioned.product),
                                    partition_template_t.render(id=id_))),
                  dag=dag,
                  name=Template('null_'+partition_template).render(id=id_))
             for id_ in partition_ids]

    def make_file(id_):
        f = File(Path(downstream_path,
                 partition_template_t.render(id=id_)))
        # patch File objects, they should not save metadata, metadata will
        # be saved once these are all done (in the gather object)

        # FIXME: this file *has* to save metadata, since it needs to act with
        # the same logic as any other task, but it cannot store it in the usual
        # place that File uses (same directory as the product). Since this
        # is a partition, mixing up partition folders and source files will
        # make the parquet loader fail for downstream dependencies, I need
        # to either modify or subclass file so that it creates the source
        # files in the upper folder
        f.save_metadata = _null
        return f

    # TODO: validate downstream product is File
    tasks = [PythonCallable(downstream_callable,
                            product=make_file(id_),
                            dag=dag,
                            name=(partition_template_w_suffix
                                  .render(id=id_)),
                            params={'upstream_key':
                                    (Template('null_'+partition_template)
                                     .render(id=id_))}
                            )
             for id_ in partition_ids]

    # gather - task that treats all partitions
    gather = Null(product=File(downstream_path),
                  dag=dag,
                  name=downstream_prefix,
                  save_metadata=True)

    # TODO: "fuse" operator that merges task chains and shows it like a single
    # task - maybe this should be like this and the dag.plot function take
    # care of simplifications for viz purposes?
    for null, task in zip(nulls, tasks):
        upstream_partitioned >> null >> task >> gather

        if upstream_other:
            upstream_other >> task

    return gather
