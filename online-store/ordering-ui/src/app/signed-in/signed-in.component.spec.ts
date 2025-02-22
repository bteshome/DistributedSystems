import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SignedInComponent } from './signed-in.component';

describe('SignedInComponent', () => {
  let component: SignedInComponent;
  let fixture: ComponentFixture<SignedInComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SignedInComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SignedInComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
