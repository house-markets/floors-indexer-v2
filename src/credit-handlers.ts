// Credit facility event handlers for Floor Markets DeFi Platform

import type { handlerContext } from 'generated'

import type { CreditFacilityContract_t, Market_t, Token_t } from '../generated/src/db/Entities.gen'
import type { LoanStatus_t } from '../generated/src/db/Enums.gen'
import { CreditFacility } from '../generated/src/Handlers.gen'
import {
  applyFacilityDeltas,
  buildUpdatedUserMarketPosition,
  fetchLoanStateEffect,
  formatAmount,
  getOrCreateAccount,
  getOrCreateUserMarketPosition,
  handlerErrorWrapper,
  normalizeAddress,
  parseLoanStateResult,
} from './helpers'

type FacilityLtvEntry = {
  previousMaxLtv: bigint
  currentMaxLtv: bigint
}

const facilityLtvHistory = new Map<string, FacilityLtvEntry>()

CreditFacility.LoanCreated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facilityContext = await loadFacilityContext(context, facilityId)

    if (!facilityContext) {
      context.log.warn(
        `[LoanCreated] Facility context missing | facilityId=${facilityId} | block=${event.block.number} | tx=${event.transaction.hash}`
      )
      return
    }

    const { facility, borrowToken, collateralToken } = facilityContext
    const borrower = await getOrCreateAccount(context, event.params.borrower_)
    const loanId = event.params.loanId_.toString()

    // Fetch floor price from Market entity (updated by FloorPriceUpdated events)
    // This is more reliable than the contract call which doesn't include floorPriceAtBorrow
    const market = await context.Market.get(facility.market_id)
    const floorPriceRaw = market?.floorPriceRaw ?? 0n

    const loanStateResult = await fetchLoanStateEffect(context.effect)({
      chainId: event.chainId,
      facilityAddress: event.srcAddress,
      loanId: event.params.loanId_.toString(),
    })
    const onChainLoan = parseLoanStateResult(loanStateResult)
    const lockedCollateralRaw = onChainLoan?.lockedIssuanceTokens ?? 0n
    const remainingDebtRaw = onChainLoan?.remainingLoanAmount ?? event.params.loanAmount_

    const borrowAmount = formatAmount(event.params.loanAmount_, borrowToken.decimals)
    const lockedCollateral = formatAmount(lockedCollateralRaw, collateralToken.decimals)
    const remainingDebt = formatAmount(remainingDebtRaw, borrowToken.decimals)
    const floorPriceAtBorrow = formatAmount(floorPriceRaw, borrowToken.decimals)

    const loan = {
      id: loanId,
      borrower_id: borrower.id,
      facility_id: facility.id,
      market_id: facility.market_id,
      lockedCollateralRaw,
      lockedCollateralFormatted: lockedCollateral.formatted,
      borrowAmountRaw: event.params.loanAmount_,
      borrowAmountFormatted: borrowAmount.formatted,
      originationFeeRaw: 0n,
      originationFeeFormatted: '0',
      remainingDebtRaw,
      remainingDebtFormatted: remainingDebt.formatted,
      floorPriceAtBorrowRaw: floorPriceRaw,
      floorPriceAtBorrowFormatted: floorPriceAtBorrow.formatted,
      status: 'ACTIVE' as LoanStatus_t,
      openedAt: timestamp,
      closedAt: undefined,
      lastUpdatedAt: timestamp,
      transactionHash: event.transaction.hash,
    }
    context.Loan.set(loan)

    const updatedFacility = applyFacilityDeltas({
      facility,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      volumeDeltaRaw: event.params.loanAmount_,
      debtDeltaRaw: event.params.loanAmount_,
      lockedCollateralDeltaRaw: lockedCollateralRaw,
      loanCountDelta: 1n,
    })
    context.CreditFacilityContract.set(updatedFacility)

    const position = await getOrCreateUserMarketPosition(
      context,
      borrower.id,
      facility.market_id,
      collateralToken.decimals
    )
    const updatedPosition = buildUpdatedUserMarketPosition(position, {
      totalDebtDelta: event.params.loanAmount_,
      lockedCollateralDelta: lockedCollateralRaw,
      issuanceTokenDecimals: collateralToken.decimals,
      reserveTokenDecimals: borrowToken.decimals,
      timestamp,
    })
    context.UserMarketPosition.set(updatedPosition)

    recordLoanStatusHistory(context, {
      loanId,
      status: 'ACTIVE',
      remainingDebtRaw,
      lockedCollateralRaw,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      transactionHash: event.transaction.hash,
      logIndex: event.logIndex,
    })

    context.log.info(
      `[LoanCreated] ✅ Loan created | loanId=${loanId} | borrower=${borrower.id} | amount=${borrowAmount.formatted}`
    )
  })
)

CreditFacility.LoanRebalanced.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facilityContext = await loadFacilityContext(context, facilityId)

    if (!facilityContext) {
      context.log.warn(
        `[LoanRebalanced] Facility context missing | facilityId=${facilityId} | block=${event.block.number} | tx=${event.transaction.hash}`
      )
      return
    }

    const { facility, borrowToken, collateralToken } = facilityContext
    const loanId = event.params.loanId_.toString()
    const loan = await context.Loan.get(loanId)

    if (!loan) {
      context.log.warn(
        `[LoanRebalanced] Loan not indexed | loanId=${loanId} | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const newLockedCollateralRaw = event.params.newLockedIssuanceTokens_
    const lockedCollateralDelta = newLockedCollateralRaw - loan.lockedCollateralRaw
    const lockedCollateral = formatAmount(newLockedCollateralRaw, collateralToken.decimals)

    const updatedLoan = {
      ...loan,
      lockedCollateralRaw: newLockedCollateralRaw,
      lockedCollateralFormatted: lockedCollateral.formatted,
      lastUpdatedAt: timestamp,
    }
    context.Loan.set(updatedLoan)

    const updatedFacility = applyFacilityDeltas({
      facility,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      lockedCollateralDeltaRaw: lockedCollateralDelta,
    })
    context.CreditFacilityContract.set(updatedFacility)

    const position = await getOrCreateUserMarketPosition(
      context,
      loan.borrower_id,
      facility.market_id,
      collateralToken.decimals
    )
    const updatedPosition = buildUpdatedUserMarketPosition(position, {
      lockedCollateralDelta,
      issuanceTokenDecimals: collateralToken.decimals,
      reserveTokenDecimals: borrowToken.decimals,
      timestamp,
    })
    context.UserMarketPosition.set(updatedPosition)

    recordLoanStatusHistory(context, {
      loanId,
      status: loan.status,
      remainingDebtRaw: loan.remainingDebtRaw,
      lockedCollateralRaw: newLockedCollateralRaw,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      transactionHash: event.transaction.hash,
      logIndex: event.logIndex,
    })

    // Note: currentFloorPrice is not in LoanRebalanced event params
    // Floor price updates are handled by FloorPriceUpdated events from the Floor contract

    context.log.info(
      `[LoanRebalanced] ✅ Loan collateral updated | loanId=${loanId} | locked=${lockedCollateral.formatted} | delta=${lockedCollateralDelta}`
    )
  })
)

CreditFacility.LoanRepaid.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facilityContext = await loadFacilityContext(context, facilityId)

    if (!facilityContext) {
      context.log.warn(
        `[LoanRepaid] Facility context missing | facilityId=${facilityId} | block=${event.block.number} | tx=${event.transaction.hash}`
      )
      return
    }

    const { facility, borrowToken, collateralToken } = facilityContext
    const loanId = event.params.loanId_.toString()
    const loan = await context.Loan.get(loanId)

    if (!loan) {
      context.log.warn(
        `[LoanRepaid] Loan not indexed | loanId=${loanId} | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const repaymentAmountRaw = event.params.repaymentAmount_
    const repaymentAmount = formatAmount(repaymentAmountRaw, borrowToken.decimals)

    const nextRemainingDebtRaw =
      loan.remainingDebtRaw > repaymentAmountRaw ? loan.remainingDebtRaw - repaymentAmountRaw : 0n
    const nextRemainingDebt = formatAmount(nextRemainingDebtRaw, borrowToken.decimals)
    const nextStatus: LoanStatus_t = nextRemainingDebtRaw === 0n ? 'REPAID' : loan.status
    const nextClosedAt = nextStatus === 'REPAID' ? timestamp : loan.closedAt

    const updatedLoan = {
      ...loan,
      remainingDebtRaw: nextRemainingDebtRaw,
      remainingDebtFormatted: nextRemainingDebt.formatted,
      status: nextStatus,
      closedAt: nextClosedAt,
      lastUpdatedAt: timestamp,
    }
    context.Loan.set(updatedLoan)

    const updatedFacility = applyFacilityDeltas({
      facility,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      debtDeltaRaw: -repaymentAmountRaw,
    })
    context.CreditFacilityContract.set(updatedFacility)

    const position = await getOrCreateUserMarketPosition(
      context,
      loan.borrower_id,
      facility.market_id,
      collateralToken.decimals
    )
    const updatedPosition = buildUpdatedUserMarketPosition(position, {
      totalDebtDelta: -repaymentAmountRaw,
      issuanceTokenDecimals: collateralToken.decimals,
      reserveTokenDecimals: borrowToken.decimals,
      timestamp,
    })
    context.UserMarketPosition.set(updatedPosition)

    recordLoanStatusHistory(context, {
      loanId,
      status: nextStatus,
      remainingDebtRaw: nextRemainingDebtRaw,
      lockedCollateralRaw: loan.lockedCollateralRaw,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      transactionHash: event.transaction.hash,
      logIndex: event.logIndex,
    })

    context.log.info(
      `[LoanRepaid] ✅ Loan updated | loanId=${loanId} | repayment=${repaymentAmount.formatted} | remainingDebt=${nextRemainingDebt.formatted}`
    )
  })
)

CreditFacility.LoanClosed.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facilityContext = await loadFacilityContext(context, facilityId)

    if (!facilityContext) {
      context.log.warn(
        `[LoanClosed] Facility context missing | facilityId=${facilityId} | block=${event.block.number} | tx=${event.transaction.hash}`
      )
      return
    }

    const { facility, borrowToken, collateralToken } = facilityContext
    const loanId = event.params.loanId_.toString()
    const loan = await context.Loan.get(loanId)

    if (!loan) {
      context.log.warn(
        `[LoanClosed] Loan not indexed | loanId=${loanId} | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const unlockedAmountRaw = event.params.issuanceTokensUnlocked_
    const nextLockedCollateralRaw =
      loan.lockedCollateralRaw > unlockedAmountRaw
        ? loan.lockedCollateralRaw - unlockedAmountRaw
        : 0n
    const lockedCollateral = formatAmount(nextLockedCollateralRaw, collateralToken.decimals)

    // LoanClosed unlocks collateral but doesn't necessarily mean debt is paid
    // Only LoanRepaid should modify remainingDebt; preserve existing debt state here
    const isDebtFullyPaid = loan.remainingDebtRaw === 0n
    const nextStatus: LoanStatus_t = isDebtFullyPaid ? 'REPAID' : loan.status
    const nextClosedAt = isDebtFullyPaid ? timestamp : loan.closedAt

    const updatedLoan = {
      ...loan,
      lockedCollateralRaw: nextLockedCollateralRaw,
      lockedCollateralFormatted: lockedCollateral.formatted,
      // Keep existing debt - only LoanRepaid should modify this
      // remainingDebtRaw and remainingDebtFormatted inherited via spread
      status: nextStatus,
      closedAt: nextClosedAt,
      lastUpdatedAt: timestamp,
    }
    context.Loan.set(updatedLoan)

    const updatedFacility = applyFacilityDeltas({
      facility,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      lockedCollateralDeltaRaw: -unlockedAmountRaw,
    })
    context.CreditFacilityContract.set(updatedFacility)

    const position = await getOrCreateUserMarketPosition(
      context,
      loan.borrower_id,
      facility.market_id,
      collateralToken.decimals
    )
    const updatedPosition = buildUpdatedUserMarketPosition(position, {
      lockedCollateralDelta: -unlockedAmountRaw,
      issuanceTokenDecimals: collateralToken.decimals,
      reserveTokenDecimals: borrowToken.decimals,
      timestamp,
    })
    context.UserMarketPosition.set(updatedPosition)

    recordLoanStatusHistory(context, {
      loanId,
      status: nextStatus,
      remainingDebtRaw: loan.remainingDebtRaw,
      lockedCollateralRaw: nextLockedCollateralRaw,
      borrowTokenDecimals: borrowToken.decimals,
      collateralTokenDecimals: collateralToken.decimals,
      timestamp,
      transactionHash: event.transaction.hash,
      logIndex: event.logIndex,
    })

    context.log.info(
      `[LoanClosed] ✅ Loan closed | loanId=${loanId} | borrower=${loan.borrower_id} | unlocked=${
        formatAmount(unlockedAmountRaw, collateralToken.decimals).formatted
      }`
    )
  })
)

CreditFacility.LoanToValueRatioUpdated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facility = await context.CreditFacilityContract.get(facilityId)

    if (!facility) {
      context.log.warn(
        `[LoanToValueRatioUpdated] Facility not indexed | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const nextRatio = event.params.newRatio_
    const history = updateFacilityLtvHistory(facility.market_id, nextRatio)
    await updateMarketMaxLtv(context, facility.market_id, nextRatio, timestamp)

    context.log.info(
      `[LoanToValueRatioUpdated] ✅ Market maxLTV updated | marketId=${facility.market_id} | previous=${history.previousMaxLtv} | next=${history.currentMaxLtv}`
    )
  })
)

CreditFacility.IssuanceTokensLocked.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    context.log.debug(
      `[IssuanceTokensLocked] Handler entry | block=${event.block.number} | tx=${event.transaction.hash}`
    )
    const user = await getOrCreateAccount(context, event.params.user_)
    context.log.info(
      `[IssuanceTokensLocked] Event observed | facility=${event.srcAddress} | user=${user.id} | amount=${event.params.amount_}`
    )
  })
)

CreditFacility.IssuanceTokensUnlocked.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    context.log.debug(
      `[IssuanceTokensUnlocked] Handler entry | block=${event.block.number} | tx=${event.transaction.hash}`
    )
    const user = await getOrCreateAccount(context, event.params.user_)
    context.log.info(
      `[IssuanceTokensUnlocked] Event observed | facility=${event.srcAddress} | user=${user.id} | amount=${event.params.amount_}`
    )
  })
)

CreditFacility.LoanTransferred.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const loanId = event.params.loanId_.toString()
    const loan = await context.Loan.get(loanId)

    if (!loan) {
      context.log.warn(
        `[LoanTransferred] Loan not indexed | loanId=${loanId} | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const newBorrower = await getOrCreateAccount(context, event.params.newBorrower_)

    const updatedLoan = {
      ...loan,
      borrower_id: newBorrower.id,
      lastUpdatedAt: timestamp,
    }
    context.Loan.set(updatedLoan)

    context.log.info(
      `[LoanTransferred] ✅ Loan transferred | loanId=${loanId} | from=${event.params.previousBorrower_} | to=${newBorrower.id}`
    )
  })
)

CreditFacility.LoansConsolidated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facilityContext = await loadFacilityContext(context, facilityId)

    if (!facilityContext) {
      context.log.warn(
        `[LoansConsolidated] Facility context missing | facilityId=${facilityId} | block=${event.block.number} | tx=${event.transaction.hash}`
      )
      return
    }

    const { facility, borrowToken, collateralToken } = facilityContext
    const borrower = await getOrCreateAccount(context, event.params.borrower_)
    const newLoanId = event.params.newLoanId_.toString()

    // Collect values from old loans first (before closing them)
    let totalBorrowAmountRaw = 0n
    let totalRemainingDebtRaw = 0n
    let totalOriginationFeeRaw = 0n
    let weightedFloorPriceSum = 0n
    let totalCollateralWeight = 0n

    for (const oldLoanId of event.params.oldLoanIds_) {
      const oldLoan = await context.Loan.get(oldLoanId.toString())
      if (oldLoan) {
        totalBorrowAmountRaw += oldLoan.borrowAmountRaw
        totalRemainingDebtRaw += oldLoan.remainingDebtRaw
        totalOriginationFeeRaw += oldLoan.originationFeeRaw
        // Weight floor price by collateral (more collateral = more weight)
        weightedFloorPriceSum += oldLoan.floorPriceAtBorrowRaw * oldLoan.lockedCollateralRaw
        totalCollateralWeight += oldLoan.lockedCollateralRaw
      }
    }

    // Calculate weighted average floor price
    const avgFloorPriceRaw =
      totalCollateralWeight > 0n ? weightedFloorPriceSum / totalCollateralWeight : 0n

    // Close old loans
    for (const oldLoanId of event.params.oldLoanIds_) {
      const oldLoan = await context.Loan.get(oldLoanId.toString())
      if (oldLoan) {
        const closedLoan = {
          ...oldLoan,
          status: 'REPAID' as LoanStatus_t,
          closedAt: timestamp,
          lastUpdatedAt: timestamp,
        }
        context.Loan.set(closedLoan)
      }
    }

    // Create new consolidated loan with summed values from old loans
    const consolidatedLoan = {
      id: newLoanId,
      borrower_id: borrower.id,
      facility_id: facility.id,
      market_id: facility.market_id,
      lockedCollateralRaw: event.params.totalLockedIssuanceTokens_,
      lockedCollateralFormatted: formatAmount(
        event.params.totalLockedIssuanceTokens_,
        collateralToken.decimals
      ).formatted,
      borrowAmountRaw: totalBorrowAmountRaw,
      borrowAmountFormatted: formatAmount(totalBorrowAmountRaw, borrowToken.decimals).formatted,
      originationFeeRaw: totalOriginationFeeRaw,
      originationFeeFormatted: formatAmount(totalOriginationFeeRaw, borrowToken.decimals).formatted,
      remainingDebtRaw: totalRemainingDebtRaw,
      remainingDebtFormatted: formatAmount(totalRemainingDebtRaw, borrowToken.decimals).formatted,
      floorPriceAtBorrowRaw: avgFloorPriceRaw,
      floorPriceAtBorrowFormatted: formatAmount(avgFloorPriceRaw, borrowToken.decimals).formatted,
      status: 'ACTIVE' as LoanStatus_t,
      openedAt: timestamp,
      closedAt: undefined,
      lastUpdatedAt: timestamp,
      transactionHash: event.transaction.hash,
    }
    context.Loan.set(consolidatedLoan)

    context.log.info(
      `[LoansConsolidated] ✅ Loans consolidated | oldLoans=${event.params.oldLoanIds_.length} | newLoanId=${newLoanId} | borrower=${borrower.id}`
    )
  })
)

CreditFacility.BorrowingFeeRateUpdated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facility = await context.CreditFacilityContract.get(facilityId)

    if (!facility) {
      context.log.warn(
        `[BorrowingFeeRateUpdated] Facility not indexed | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const updatedFacility = {
      ...facility,
      lastUpdatedAt: timestamp,
    }
    context.CreditFacilityContract.set(updatedFacility)

    context.log.info(
      `[BorrowingFeeRateUpdated] ✅ Borrowing fee rate updated | facilityId=${facilityId} | newFeeRate=${event.params.newFeeRate_.toString()}`
    )
  })
)

CreditFacility.MaxLeverageUpdated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facility = await context.CreditFacilityContract.get(facilityId)

    if (!facility) {
      context.log.warn(
        `[MaxLeverageUpdated] Facility not indexed | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const updatedFacility = {
      ...facility,
      lastUpdatedAt: timestamp,
    }
    context.CreditFacilityContract.set(updatedFacility)

    context.log.info(
      `[MaxLeverageUpdated] ✅ Max leverage updated | facilityId=${facilityId} | newMaxLeverage=${event.params.newMaxLeverage_.toString()}`
    )
  })
)

CreditFacility.DynamicFeeCalculatorUpdated.handler(
  handlerErrorWrapper(async ({ event, context }) => {
    const facilityId = normalizeAddress(event.srcAddress)
    const timestamp = BigInt(event.block.timestamp)
    const facility = await context.CreditFacilityContract.get(facilityId)

    if (!facility) {
      context.log.warn(
        `[DynamicFeeCalculatorUpdated] Facility not indexed | facilityId=${facilityId} | tx=${event.transaction.hash}`
      )
      return
    }

    const updatedFacility = {
      ...facility,
      lastUpdatedAt: timestamp,
    }
    context.CreditFacilityContract.set(updatedFacility)

    context.log.info(
      `[DynamicFeeCalculatorUpdated] ✅ Fee calculator updated | facilityId=${facilityId} | newCalculator=${event.params.newCalculator_}`
    )
  })
)

type FacilityContext = {
  facility: CreditFacilityContract_t
  borrowToken: Token_t
  collateralToken: Token_t
}

async function loadFacilityContext(
  context: handlerContext,
  facilityId: string
): Promise<FacilityContext | null> {
  const facility = await context.CreditFacilityContract.get(facilityId)
  if (!facility) {
    return null
  }

  const borrowToken = await context.Token.get(facility.borrowToken_id)
  const collateralToken = await context.Token.get(facility.collateralToken_id)

  if (!borrowToken || !collateralToken) {
    return null
  }

  return { facility, borrowToken, collateralToken }
}

function recordLoanStatusHistory(
  context: handlerContext,
  params: {
    loanId: string
    status: LoanStatus_t
    remainingDebtRaw: bigint
    lockedCollateralRaw: bigint
    borrowTokenDecimals: number
    collateralTokenDecimals: number
    timestamp: bigint
    transactionHash: string
    logIndex: number
  }
) {
  const historyId = `${params.loanId}-${params.transactionHash}-${params.logIndex}`
  const entry = {
    id: historyId,
    loan_id: params.loanId,
    status: params.status,
    remainingDebtRaw: params.remainingDebtRaw,
    remainingDebtFormatted: formatAmount(params.remainingDebtRaw, params.borrowTokenDecimals)
      .formatted,
    lockedCollateralRaw: params.lockedCollateralRaw,
    lockedCollateralFormatted: formatAmount(
      params.lockedCollateralRaw,
      params.collateralTokenDecimals
    ).formatted,
    timestamp: params.timestamp,
    transactionHash: params.transactionHash,
  }

  context.LoanStatusHistory.set(entry)
}

function updateFacilityLtvHistory(marketId: string, nextRatio: bigint): FacilityLtvEntry {
  const existing = facilityLtvHistory.get(marketId) ?? {
    previousMaxLtv: 0n,
    currentMaxLtv: 0n,
  }

  const entry: FacilityLtvEntry = {
    previousMaxLtv: existing.currentMaxLtv,
    currentMaxLtv: nextRatio,
  }

  facilityLtvHistory.set(marketId, entry)
  return entry
}

async function updateMarketMaxLtv(
  context: handlerContext,
  marketId: string,
  nextRatio: bigint,
  timestamp: bigint
): Promise<void> {
  const market = await context.Market.get(marketId)
  if (!market) {
    context.log.warn(`[LoanToValueRatioUpdated] Market not found | marketId=${marketId}`)
    return
  }

  context.Market.set({
    ...market,
    maxLTV: nextRatio,
    lastUpdatedAt: timestamp,
  })
}

async function updateMarketFloorPriceViaFacility(
  context: handlerContext,
  marketId: string,
  nextFloorPriceRaw: bigint,
  reserveTokenDecimals: number,
  timestamp: bigint
): Promise<void> {
  const market = (await context.Market.get(marketId)) as Market_t | undefined
  if (!market) {
    context.log.warn(
      `[LoanRebalanced] Market not found while updating floor price | marketId=${marketId}`
    )
    return
  }

  const floorPriceAmount = formatAmount(nextFloorPriceRaw, reserveTokenDecimals)
  const nextInitialFloorPriceRaw =
    market.initialFloorPriceRaw > 0n ? market.initialFloorPriceRaw : nextFloorPriceRaw
  const nextInitialFloorPriceFormatted =
    market.initialFloorPriceRaw > 0n
      ? market.initialFloorPriceFormatted
      : floorPriceAmount.formatted

  context.Market.set({
    ...market,
    floorPriceRaw: nextFloorPriceRaw,
    floorPriceFormatted: floorPriceAmount.formatted,
    initialFloorPriceRaw: nextInitialFloorPriceRaw,
    initialFloorPriceFormatted: nextInitialFloorPriceFormatted,
    lastUpdatedAt: timestamp,
  })
}
