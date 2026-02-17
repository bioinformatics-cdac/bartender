
import asyncio
import os
from unittest.mock import AsyncMock, patch
from bartender.schedulers.arq import _exec, ArqSchedulerConfig, JobDescription

async def test_exec():
    # Mock dependencies
    ctx = {"config": ArqSchedulerConfig()}
    description = JobDescription(job_dir="/tmp", command="echo hello")
    
    # Test case 1: USE_K8S is not set (default) -> should call _exec_local
    with patch("bartender.schedulers.arq._exec_local", new_callable=AsyncMock) as mock_local, \
         patch("bartender.schedulers.arq._exec_k8s", new_callable=AsyncMock) as mock_k8s:
        if "USE_K8S" in os.environ:
            del os.environ["USE_K8S"]
            
        await _exec(ctx, description)
        
        mock_local.assert_called_once_with(description)
        mock_k8s.assert_not_called()
        print("✅ Test 1 Passed: USE_K8S unset -> _exec_local called")

    # Test case 2: USE_K8S="True" -> should call _exec_k8s
    with patch("bartender.schedulers.arq._exec_local", new_callable=AsyncMock) as mock_local, \
         patch("bartender.schedulers.arq._exec_k8s", new_callable=AsyncMock) as mock_k8s:
        os.environ["USE_K8S"] = "True"
            
        await _exec(ctx, description)
        
        mock_k8s.assert_called_once_with(ctx, description, ctx["config"])
        mock_local.assert_not_called()
        print("✅ Test 2 Passed: USE_K8S='True' -> _exec_k8s called")

    # Test case 3: USE_K8S="False" -> should call _exec_local
    with patch("bartender.schedulers.arq._exec_local", new_callable=AsyncMock) as mock_local, \
         patch("bartender.schedulers.arq._exec_k8s", new_callable=AsyncMock) as mock_k8s:
        os.environ["USE_K8S"] = "False"
            
        await _exec(ctx, description)
        
        mock_local.assert_called_once_with(description)
        mock_k8s.assert_not_called()
        print("✅ Test 3 Passed: USE_K8S='False' -> _exec_local called")

asyncio.run(test_exec())
