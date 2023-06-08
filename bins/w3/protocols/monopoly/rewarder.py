from web3 import Web3
from bins.w3.protocols.gamma.rewarder import gamma_rewarder


# Duo-> https://monopoly.finance/
class duo_masterchef_v1(gamma_rewarder):
    # TODO: https://arbiscan.io/address/0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba#code
    # pools affected:
    #        wide pool pid 25   0xD75faCEC47A40b29522FA2515AAf269a9Ce7049e
    #        narrow pool pid 26 0xEF207FbF72710021a838935a6574e62CFfAa7C10

    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "duoMaster_rewarder"
        self._abi_path = abi_path or "data/abi/duo/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def earningPerYear(self) -> int:
        """earning per year

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("earningPerYear")

    def earningPerYearToMonopoly(self, pid: int) -> int:
        """earning per year to monopoly

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("earningPerYearToMonopoly", None, pid)

    @property
    def earningReferral(self) -> str:
        """earning referral

        Returns:
            str: address
        """
        return self.call_function_autoRpc("earningReferral")

    @property
    def earningToken(self) -> str:
        """earning token

        Returns:
            str: address
        """
        return self.call_function_autoRpc("earningToken")

    @property
    def earningsPerSecond(self) -> int:
        """earnings per second

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("earningsPerSecond")

    @property
    def endTime(self) -> int:
        """end time

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("endTime")

    @property
    def startTime(self) -> int:
        """start time

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("startTime")

    def lpPrice(self, address: str) -> int:
        """lp price

        Args:
            address (str):

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc(
            "lpPrice", None, Web3.toChecksumAddress(address)
        )

    def poolInfo(self, pid: int) -> tuple[int, int, int, int, int]:
        """

        Args:
            pid (int): pool index

        Returns:
            tuple:
                want:  hypervisor address
                strategy: address
                allocPoint uint256 — allocation points assigned to the pool.
                lastRewardTime uint256
                accEarningPerShare uint256
                totalShares uint256
                lpPerShare uint256
                depositFeeBP uint16
                withdrawFeeBP uint16
                isWithdrawFee bool
        """
        return self.call_function_autoRpc("poolInfo", None, pid)

    @property
    def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("poolLength")

    @property
    def totalAllocPoint(self) -> int:
        """total allocation points

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalAllocPoint")

    def totalLP(self, pid: int) -> int:
        """total lp

        Args:
            pid (int): pool index

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalLP", None, pid)

    def totalShares(self, pid: int) -> int:
        """total shares

        Args:
            pid (int): pool index

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalShares", None, pid)
