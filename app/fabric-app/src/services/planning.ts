export interface ScenarioNumbers {
  baseline_units: number;
  unit_price_cents: number;
  unit_cost_cents: number;
  discount_percent: number;
  uplift_percent: number;
}

export function validateScenarioNumbers(input: ScenarioNumbers): string[] {
  const errors: string[] = [];
  for (const key of ['baseline_units', 'unit_price_cents', 'unit_cost_cents'] as const) {
    if (!Number.isSafeInteger(input[key]) || input[key] < 1 || input[key] > 1_000_000) {
      errors.push(`${key.replaceAll('_', ' ')} must be a whole number from 1 to 1,000,000.`);
    }
  }
  if (!Number.isInteger(input.discount_percent) || input.discount_percent < 0 || input.discount_percent > 70) {
    errors.push('Discount must be a whole percentage from 0 to 70.');
  }
  if (!Number.isInteger(input.uplift_percent) || input.uplift_percent < -30 || input.uplift_percent > 100) {
    errors.push('Assumed unit uplift must be a whole percentage from -30 to 100.');
  }
  return errors;
}

export function scenarioImpact(input: ScenarioNumbers) {
  const errors = validateScenarioNumbers(input);
  if (errors.length) throw new Error(errors.join(' '));
  const promoPriceCents = Math.round(input.unit_price_cents * (1 - input.discount_percent / 100));
  const estimatedUnits = Math.round(input.baseline_units * (1 + input.uplift_percent / 100));
  const baselineRevenueCents = input.baseline_units * input.unit_price_cents;
  const baselineProfitCents = input.baseline_units * (input.unit_price_cents - input.unit_cost_cents);
  const revenueCents = estimatedUnits * promoPriceCents;
  const profitCents = estimatedUnits * (promoPriceCents - input.unit_cost_cents);
  const discountInvestmentCents = input.baseline_units * (input.unit_price_cents - promoPriceCents);
  const volumeContributionCents = (estimatedUnits - input.baseline_units) * (promoPriceCents - input.unit_cost_cents);
  const breakEvenUplift = promoPriceCents > input.unit_cost_cents
    ? Math.ceil(((input.unit_price_cents - input.unit_cost_cents) / (promoPriceCents - input.unit_cost_cents) - 1) * 100)
    : null;
  return {
    promoPriceCents, estimatedUnits, baselineRevenueCents, baselineProfitCents,
    revenueCents, profitCents, discountInvestmentCents, volumeContributionCents,
    breakEvenUplift, profitChangeCents: profitCents - baselineProfitCents,
  };
}

export function campaignDays(start: Date, end: Date): number {
  const difference = (end.getTime() - start.getTime()) / 86_400_000;
  if (!Number.isFinite(difference) || !Number.isInteger(difference) || difference < 0 || difference >= 84) {
    throw new Error('Choose a campaign lasting 1 to 84 days, with the end on or after the start.');
  }
  return difference + 1;
}
