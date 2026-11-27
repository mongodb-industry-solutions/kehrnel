"""Request-scoped RPS ingestion dependencies."""
import logging
from types import SimpleNamespace
from fastapi import HTTPException
from kehrnel.api.bridge.app.core.database import resolve_active_openehr_context
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy
from kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy import RPSDualIBMStrategy

logger = logging.getLogger(__name__)

async def request_flattener(request, *, expected=None):
    context = await resolve_active_openehr_context(request, ensure_ingestion=False)
    activation = context['activation']
    strategies = {'openehr.rps_dual': RPSDualStrategy, 'openehr.rps_dual_ibm': RPSDualIBMStrategy}
    if activation.strategy_id not in strategies or (expected and activation.strategy_id != expected):
        raise HTTPException(status_code=409, detail='The active environment does not use the required operational strategy.')
    if 'synthetic_flattener' not in context:
        try:
            context['synthetic_flattener'] = await strategies[activation.strategy_id]()._build_flattener_for_context(
                StrategyContext(environment_id=context['env_id'], config=activation.config,
                                adapters={'storage': SimpleNamespace(db=context['db'])}))
        except Exception as exc:
            logger.error('Ingestion initialization failed (%s)', type(exc).__name__)
            raise HTTPException(status_code=503, detail='Ingestion could not be initialized. Check the environment configuration.') from exc
    return context['synthetic_flattener']
