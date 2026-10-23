import { ApprovalDecision } from './ApprovalDecision.js';
import { MerchandisingView } from './MerchandisingView.js';
import { PromotionScenario } from './PromotionScenario.js';
import { ReplenishmentProposal } from './ReplenishmentProposal.js';

export type ApprovalsAppSchema = {
  ReplenishmentProposal: ReplenishmentProposal;
  ApprovalDecision: ApprovalDecision;
  MerchandisingView: MerchandisingView;
  PromotionScenario: PromotionScenario;
};

export const schema = [ReplenishmentProposal, ApprovalDecision, MerchandisingView, PromotionScenario];