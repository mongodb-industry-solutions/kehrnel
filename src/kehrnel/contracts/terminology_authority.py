"""Non-secret authority identity retained in terminology execution evidence."""
from typing import Literal, Optional
from pydantic import Field, model_validator
from kehrnel.contracts.artifacts import ContractModel

class TerminologyAuthorityEvidence(ContractModel):
    mode: Literal['hosted_reference', 'customer_owned']
    id: Optional[str] = Field(default=None, max_length=128, exclude_if=lambda value: value is None)
    release_id: Optional[str] = Field(default=None, max_length=128, exclude_if=lambda value: value is None)
    projection_id: Optional[str] = Field(default=None, max_length=128, exclude_if=lambda value: value is None)
    read_only: Optional[bool] = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode='after')
    def hosted_is_pinned_and_read_only(self):
        if self.mode == 'hosted_reference' and not (self.id and self.release_id and self.projection_id and self.read_only is True):
            raise ValueError('Hosted authority evidence must identify a pinned read-only release and projection.')
        return self
