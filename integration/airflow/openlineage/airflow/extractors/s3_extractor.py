# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.common.dataset import Dataset, Source

log = logging.getLogger(__name__)


class S3CopyObjectExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['S3CopyObjectOperator']

    def extract(self) -> Optional[TaskMetadata]:

        input_object = Dataset(
            name=self.operator.source_bucket_key,
            source=Source(
                scheme="s3",
                authority=self.operator.source_bucket_name,
                connection_url=f"s3://{self.operator.source_bucket_name}/{self.operator.source_bucket_key}"
            )
        )

        output_object = Dataset(
            name=self.operator.dest_bucket_key,
            source=Source(
                scheme="s3",
                authority=self.operator.dest_bucket_name,
                connection_url=f"s3://{self.operator.dest_bucket_name}/{self.operator.dest_bucket_key}"
            )
        )

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[input_object.to_openlineage_dataset()],
            outputs=[output_object.to_openlineage_dataset()],
        )
