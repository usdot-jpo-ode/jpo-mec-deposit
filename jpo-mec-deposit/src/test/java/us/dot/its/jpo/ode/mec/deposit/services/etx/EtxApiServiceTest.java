package us.dot.its.jpo.ode.mec.deposit.services.etx;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;

@ExtendWith(MockitoExtension.class)
class EtxApiServiceTest {

  @Mock
  private EtxPartnerClient mockPartnerApi;

  @Mock
  private EtxTokenManager mockTokenManager;

  private EtxApiService etxApiService;

  @BeforeEach
  void setUp() {
    etxApiService = new EtxApiService(mockPartnerApi, mockTokenManager);
  }

  @Test
  void clearTim_WhenEnabled_ShouldClearTim() {
    // Arrange
    ReflectionTestUtils.setField(etxApiService, "clearTimEnabled", true);
    when(mockTokenManager.getValidToken()).thenReturn("test-token");

    // Act
    etxApiService.clearTim();

    // Assert
    verify(mockTokenManager).getValidToken();
    verify(mockPartnerApi).clearTim("test-token");
  }

  @Test
  void clearTim_WhenDisabled_ShouldNotClearTim() {
    // Arrange
    ReflectionTestUtils.setField(etxApiService, "clearTimEnabled", false);

    // Act
    etxApiService.clearTim();

    // Assert
    verify(mockTokenManager, never()).getValidToken();
    verify(mockPartnerApi, never()).clearTim(any());
  }
}
